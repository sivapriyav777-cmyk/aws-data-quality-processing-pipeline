
import boto3
import csv
import io
import os
import json
import re
from datetime import datetime
from urllib.parse import unquote_plus

s3 = boto3.client("s3")

BUCKET_NAME = os.environ["BUCKET_NAME"]


def validate_record(row, seen_ids):

    errors = []

    transaction_id = row.get("transaction_id", "").strip()
    customer_id = row.get("customer_id", "").strip()
    email = row.get("email", "").strip()
    amount = row.get("amount", "").strip()
    date = row.get("date", "").strip()

    if not transaction_id:
        errors.append("Missing transaction ID")
    elif transaction_id in seen_ids:
        errors.append("Duplicate transaction ID")

    if not customer_id:
        errors.append("Missing customer ID")

    if not re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", email):
        errors.append("Invalid email")

    try:
        if float(amount) <= 0:
            errors.append("Invalid transaction amount")
    except (ValueError, TypeError):
        errors.append("Invalid transaction amount")

    try:
        datetime.strptime(date, "%Y-%m-%d")
    except ValueError:
        errors.append("Invalid date format")

    return errors


def lambda_handler(event, context):

    try:
        record = event["Records"][0]

        bucket = record["s3"]["bucket"]["name"]
        key = unquote_plus(record["s3"]["object"]["key"])

        if not key.startswith("raw/"):
            return {"statusCode": 400, "body": "Invalid input folder"}

        file_object = s3.get_object(
            Bucket=bucket,
            Key=key
        )

        content = file_object["Body"].read().decode("utf-8-sig")

        reader = csv.DictReader(io.StringIO(content))

        required = {
            "transaction_id", "customer_id",
            "email", "amount", "date"
        }

        if not reader.fieldnames or not required.issubset(
            set(reader.fieldnames)
        ):
            raise ValueError("Required CSV columns are missing")

        valid_records = []
        invalid_records = []
        seen_ids = set()

        for row in reader:

            errors = validate_record(row, seen_ids)

            transaction_id = row.get("transaction_id", "").strip()

            if transaction_id:
                seen_ids.add(transaction_id)

            if errors:
                row["errors"] = "; ".join(errors)
                invalid_records.append(row)
            else:
                valid_records.append(row)

        def create_csv(records, fields):

            output = io.StringIO()
            writer = csv.DictWriter(
                output,
                fieldnames=fields,
                extrasaction="ignore"
            )

            writer.writeheader()
            writer.writerows(records)

            return output.getvalue()

        valid_fields = reader.fieldnames
        invalid_fields = valid_fields + ["errors"]

        if valid_records:
            s3.put_object(
                Bucket=BUCKET_NAME,
                Key="processed/valid_transactions.csv",
                Body=create_csv(valid_records, valid_fields)
            )

        if invalid_records:
            s3.put_object(
                Bucket=BUCKET_NAME,
                Key="rejected/invalid_transactions.csv",
                Body=create_csv(invalid_records, invalid_fields)
            )

        result = {
            "valid_count": len(valid_records),
            "invalid_count": len(invalid_records)
        }

        print(json.dumps(result))

        return {
            "statusCode": 200,
            "body": json.dumps(result)
        }

    except Exception as error:

        print(f"Processing failed: {str(error)}")
        raise
