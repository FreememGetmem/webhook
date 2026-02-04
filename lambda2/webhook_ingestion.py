import json
import boto3
import os
import logging
import time
import random
from datetime import datetime
from botocore.exceptions import ClientError

# -----------------------
# Logging
# -----------------------
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# -----------------------
# AWS Clients
# -----------------------
s3 = boto3.client("s3")

# -----------------------
# Env
# -----------------------
BUCKET_NAME = os.environ["BUCKET_NAME"]
SOURCE_PREFIX = os.environ.get("SOURCE_PREFIX", "source/")

# -----------------------
# Custom Exceptions
# -----------------------
class ValidationError(Exception):
    pass

class RetryableError(Exception):
    pass

# -----------------------
# Retry Decorator
# -----------------------
def with_retry(max_attempts=3, base_delay=0.5):
    def decorator(func):
        def wrapper(*args, **kwargs):
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except RetryableError as e:
                    if attempt == max_attempts:
                        logger.error("Max retries reached", exc_info=True)
                        raise
                    sleep_time = base_delay * (2 ** (attempt - 1))
                    sleep_time += random.uniform(0, 0.3)
                    logger.warning(
                        f"Retrying after error (attempt {attempt}/{max_attempts}): {e}",
                        extra={"sleep": sleep_time}
                    )
                    time.sleep(sleep_time)
        return wrapper
    return decorator

# -----------------------
# Lambda Handler
# -----------------------
def lambda_handler(event, context):
    request_id = context.aws_request_id
    logger.info("Webhook received", extra={"request_id": request_id})

    try:
        body = parse_body(event)
        validate_webhook(body)

        lead_data = extract_lead_data(body)
        s3_key = store_lead_in_s3(lead_data, body)

        logger.info(
            "Lead stored successfully",
            extra={
                "request_id": request_id,
                "lead_id": lead_data["lead_id"],
                "s3_key": s3_key,
            },
        )

        return create_response(200, {
            "message": "Lead received and stored successfully",
            "lead_id": lead_data["lead_id"],
            "s3_key": s3_key
        })

    except ValidationError as e:
        logger.warning("Validation failed", extra={"request_id": request_id, "error": str(e)})
        return create_response(400, {"error": str(e)})

    except Exception as e:
        logger.error("Unhandled error", exc_info=True, extra={"request_id": request_id})
        return create_response(500, {"error": "Internal server error"})

# -----------------------
# Helpers
# -----------------------
def parse_body(event):
    try:
        if "body" in event:
            return json.loads(event["body"]) if isinstance(event["body"], str) else event["body"]
        return event
    except json.JSONDecodeError:
        raise ValidationError("Invalid JSON payload")

def validate_webhook(body):
    event = body.get("event")
    if not event:
        raise ValidationError("Missing event object")

    if event.get("action") != "created":
        raise ValidationError("Unsupported event action")

    for field in ("lead_id", "data"):
        if field not in event:
            raise ValidationError(f"Missing required field: {field}")

def extract_lead_data(body):
    event = body["event"]
    data = event.get("data", {})

    lead_id = event.get("lead_id")
    if not lead_id:
        raise ValidationError("lead_id is required")

    return {
        "lead_id": lead_id,
        "display_name": data.get("display_name", "Unknown"),
        "status_label": data.get("status_label", "Unknown"),
        "date_created": data.get("date_created"),
        "subscription_id": body.get("subscription_id"),
        "event_id": event.get("id"),
    }

# -----------------------
# S3 Storage (with retry)
# -----------------------
@with_retry(max_attempts=3)
def store_lead_in_s3(lead_data, full_body):
    lead_id = lead_data["lead_id"]
    s3_key = f"{SOURCE_PREFIX}crm_event_{lead_id}.json"

    payload = {
        **full_body,
        "processed_at": datetime.utcnow().isoformat(),
        "extracted_lead_data": lead_data,
    }

    try:
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=json.dumps(payload, indent=2),
            ContentType="application/json",
            Metadata={
                "lead_id": lead_id,
                "processed_at": datetime.utcnow().isoformat(),
            },
        )
        return s3_key

    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        logger.error("S3 put_object failed", extra={"error_code": error_code})

        # Retry only transient errors
        if error_code in ("Throttling", "SlowDown", "RequestTimeout"):
            raise RetryableError(error_code)

        raise

# -----------------------
# HTTP Response
# -----------------------
def create_response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        },
        "body": json.dumps(body),
    }
