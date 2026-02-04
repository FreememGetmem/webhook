import json
import boto3
import os
import logging
import urllib3
import time
from datetime import datetime
from botocore.exceptions import ClientError

# --------------------------------------------------
# Logging
# --------------------------------------------------
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# --------------------------------------------------
# AWS Clients
# --------------------------------------------------
s3 = boto3.client("s3")
sns = boto3.client("sns")
secrets = boto3.client("secretsmanager")

http = urllib3.PoolManager(
    retries=urllib3.Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
    ),
    timeout=urllib3.Timeout(connect=2.0, read=5.0),
)

# --------------------------------------------------
# Environment
# --------------------------------------------------
BUCKET_NAME = os.environ["BUCKET_NAME"]
SOURCE_PREFIX = os.environ.get("SOURCE_PREFIX", "source/")
TARGET_PREFIX = os.environ.get("TARGET_PREFIX", "target/")
LOOKUP_BUCKET = os.environ.get("LOOKUP_BUCKET", "dea-lead-owner")

SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN", "")
SLACK_SECRET_NAME = os.environ.get("SLACK_SECRET_NAME", "")
USE_SLACK = os.environ.get("USE_SLACK", "false").lower() == "true"
USE_EMAIL = os.environ.get("USE_EMAIL", "false").lower() == "true"


# ==================================================
# Lambda Entry Point
# ==================================================
def lambda_handler(event, context):
    logger.info(
        "Lambda invoked",
        extra={
            "request_id": context.aws_request_id,
            "records": len(event.get("Records", [])),
            "use_email": USE_EMAIL,
            "use_slack": USE_SLACK,
        },
    )

    for record in event.get("Records", []):
        try:
            body = json.loads(record["body"])
            for s3_record in body.get("Records", []):
                process_lead(s3_record)
        except Exception:
            logger.exception("Record processing failed — allowing SQS retry")
            raise  # IMPORTANT: allow retry / DLQ

    return {"status": "ok"}


# ==================================================
# Core Processing
# ==================================================
def process_lead(s3_record):
    bucket = s3_record["s3"]["bucket"]["name"]
    key = s3_record["s3"]["object"]["key"]

    logger.info("Processing S3 object", extra={"bucket": bucket, "key": key})
    lead_data = read_s3_json(bucket, key)
    lead_id = lead_data.get("event", {}).get("lead_id")
    if not lead_id:
        raise ValueError("Missing lead_id in payload")

    owner_data = lookup_lead_owner(lead_id)
    enriched = enrich_lead_data(lead_data, owner_data)

    store_enriched_data(enriched, lead_id)

    # Notifications should never break the pipeline
    try:
        send_notifications(enriched)
    except Exception:
        logger.exception("Notification failure (non-blocking)")


# ==================================================
# Helpers
# ==================================================
def read_s3_json(bucket, key, retries=3):
    for attempt in range(retries):
        try:
            obj = s3.get_object(Bucket=bucket, Key=key)
            return json.loads(obj["Body"].read())
        except ClientError :
            if attempt == retries - 1:
                raise
            logger.warning("S3 read retry", extra={"attempt": attempt + 1})
            time.sleep(2 ** attempt)


def lookup_lead_owner(lead_id):
    url = f"https://{LOOKUP_BUCKET}.s3.us-east-1.amazonaws.com/{lead_id}.json"
    logger.info("Looking up lead owner", extra={"url": url})

    try:
        response = http.request("GET", url)
        if response.status == 200:
            return json.loads(response.data)
        logger.warning("Owner lookup not found", extra={"status": response.status})
    except Exception:
        logger.exception("Owner lookup failed")

    return None


def enrich_lead_data(lead_data, owner_data):
    event = lead_data.get("event", {})
    data = event.get("data", {})

    owner_data = owner_data or {
        "lead_email": "unknown@example.com",
        "lead_owner": "Unassigned",
        "funnel": "Unknown",
    }

    return {
        "lead_id": event.get("lead_id"),
        "display_name": data.get("display_name", "Unknown"),
        "status_label": data.get("status_label", "Unknown"),
        "date_created": data.get("date_created"),
        "lead_email": owner_data["lead_email"],
        "lead_owner": owner_data["lead_owner"],
        "funnel": owner_data["funnel"],
        "enriched_at": datetime.utcnow().isoformat(),
    }


def store_enriched_data(enriched, lead_id):
    key = f"{TARGET_PREFIX}enriched_{lead_id}.json"
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=key,
        Body=json.dumps(enriched, indent=2),
        ContentType="application/json",
    )
    logger.info("Enriched data stored", extra={"key": key})


# ==================================================
# Notifications
# ==================================================
def send_notifications(enriched):
    if USE_SLACK:
        send_slack(enriched)
    if USE_EMAIL and SNS_TOPIC_ARN:
        send_email(enriched)


def send_slack(enriched):
    secret = secrets.get_secret_value(SecretId=SLACK_SECRET_NAME)
    webhook_url = json.loads(secret["SecretString"])["webhook_url"]

    payload = {
        "text": f"🎯 New Lead: {enriched['display_name']}",
    }

    response = http.request(
        "POST",
        webhook_url,
        body=json.dumps(payload),
        headers={"Content-Type": "application/json"},
    )

    logger.info("Slack response", extra={"status": response.status})
    if response.status >= 400:
        raise RuntimeError("Slack webhook failed")


def send_email(enriched):
    sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=f"🎯 New Lead: {enriched['display_name']}",
        Message=json.dumps(enriched, indent=2),
    )
    logger.info("SNS email published")
