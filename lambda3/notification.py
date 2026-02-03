import json
import urllib3
import boto3

http = urllib3.PoolManager()
s3 = boto3.client("s3")


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

    encoded_body = json.dumps(response_body).encode("utf-8")

    http.request(
        "PUT",
        event["ResponseURL"],
        body=encoded_body,
        headers={
            "content-type": "",
            "content-length": str(len(encoded_body))
        }
    )


def handler(event, context):
    try:
        print("Event:", json.dumps(event))

        if event["RequestType"] in ["Create", "Update"]:
            bucket = event["ResourceProperties"]["BucketName"]
            queue_arn = event["ResourceProperties"]["QueueArn"]
            prefix = event["ResourceProperties"].get("Prefix", "")

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

        elif event["RequestType"] == "Delete":
            bucket = event["ResourceProperties"]["BucketName"]
            s3.put_bucket_notification_configuration(
                Bucket=bucket,
                NotificationConfiguration={}
            )

        send_response(event, context, "SUCCESS")

    except Exception as e:
        print("ERROR:", str(e))
        send_response(event, context, "FAILED", str(e))
