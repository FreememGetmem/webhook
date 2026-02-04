import json
import time
import logging
import urllib3
import boto3
from botocore.exceptions import ClientError

# -----------------------------
# Clients & globals
# -----------------------------
http = urllib3.PoolManager()
s3 = boto3.client("s3")

logger = logging.getLogger()
logger.setLevel(logging.INFO)

MAX_RETRIES = 5
RETRY_DELAY = 2  # seconds


# -----------------------------
# CloudFormation Response
# -----------------------------
def send_response(event, context, status, reason=None):
    response_body = {
        "Status": status,
        "Reason": reason or f"See CloudWatch Logs: {context.log_stream_name}",
        "PhysicalResourceId": context.log_stream_name,
        "StackId": event["StackId"],
        "RequestId": event["RequestId"],
        "LogicalResourceId": event["LogicalResourceId"],
        "Data": {}
    }

    body = json.dumps(response_body).encode("utf-8")

    try:
        http.request(
            "PUT",
            event["ResponseURL"],
            body=body,
            headers={
                "content-type": "",
                "content-length": str(len(body))
            },
            retries=False,
            timeout=urllib3.Timeout(connect=5.0, read=5.0),
        )
        logger.info("CloudFormation response sent: %s", status)
    except Exception:
        # At this point we can only log — CFN timeout is unavoidable
        logger.error("FAILED to send CloudFormation response", exc_info=True)


# -----------------------------
# Retry helper
# -----------------------------
def with_retries(func, *, context, description):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info("Attempt %d/%d: %s", attempt, MAX_RETRIES, description)
            return func()
        except ClientError as e:
            code = e.response["Error"]["Code"]
            logger.warning("AWS error (%s): %s", code, str(e))

            # Stop retrying if Lambda is about to timeout
            if context.get_remaining_time_in_millis() < 5000:
                logger.error("Not enough time left to retry")
                raise

            if attempt == MAX_RETRIES:
                raise

            time.sleep(RETRY_DELAY * attempt)


# -----------------------------
# Main handler
# -----------------------------
def handler(event, context):
    logger.info("Received event: %s", json.dumps(event))

    try:
        request_type = event["RequestType"]
        props = event.get("ResourceProperties", {})

        bucket = props.get("BucketName")
        queue_arn = props.get("QueueArn")
        prefix = props.get("Prefix", "")

        if not bucket:
            raise ValueError("BucketName is required")

        if request_type in ("Create", "Update"):
            if not queue_arn:
                raise ValueError("QueueArn is required for Create/Update")

            def configure_notification():
                s3.put_bucket_notification_configuration(
                    Bucket=bucket,
                    NotificationConfiguration={
                        "QueueConfigurations": [
                            {
                                "QueueArn": queue_arn,
                                "Events": ["s3:ObjectCreated:*"],
                                "Filter": {
                                    "Key": {
                                        "FilterRules": [
                                            {"Name": "prefix", "Value": prefix}
                                        ]
                                    }
                                }
                            }
                        ]
                    }
                )

            with_retries(
                configure_notification,
                context=context,
                description=f"Configure S3 notifications for bucket {bucket}",
            )

        elif request_type == "Delete":

            def clear_notification():
                s3.put_bucket_notification_configuration(
                    Bucket=bucket,
                    NotificationConfiguration={}
                )

            with_retries(
                clear_notification,
                context=context,
                description=f"Clear S3 notifications for bucket {bucket}",
            )

        else:
            logger.warning("Unknown RequestType: %s", request_type)

        send_response(event, context, "SUCCESS")

    except Exception as e:
        logger.error("Custom resource FAILED", exc_info=True)
        send_response(event, context, "FAILED", str(e))
