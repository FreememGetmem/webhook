import json
import boto3
import os
import logging
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')

BUCKET_NAME = os.environ.get('BUCKET_NAME')
SOURCE_PREFIX = os.environ.get('SOURCE_PREFIX', 'source/')


def lambda_handler(event, context):
    try:
        logger.info("Received webhook event")
        if 'body' in event:
            body = json.loads(event['body']) if isinstance(event['body'], str) else event['body']
        else:
            body = event
        if not validate_webhook(body):
            logger.error("Invalid webhook payload")
            return create_response(400, {"error": "Invalid webhook payload"})
        lead_data = extract_lead_data(body)
        if not lead_data:
            logger.error("Could not extract lead data")
            return create_response(400, {"error": "Invalid lead data"})
        s3_key = store_lead_in_s3(lead_data, body)
        logger.info(f"Successfully stored lead: {lead_data['lead_id']} at {s3_key}")
        return create_response(200, {
                                        "message": "Lead received and stored successfully",
                                        "lead_id": lead_data['lead_id'],
                                        "s3_key": s3_key
                                                })
    except Exception as e:
        logger.error(f"Error: {str(e)}", exc_info=True)
        return create_response(500, {"error": "Internal server error"})


def validate_webhook(body):
    try:
        if 'event' not in body:
            return False
        event = body['event']
        required_fields = ['lead_id', 'action', 'data']
        for field in required_fields:
            if field not in event:
                return False
            if event.get('action') != 'created':
                return False
        return True
    except Exception as e:
        logger.error(f"Validation error: {str(e)}")
        return False


def extract_lead_data(body):
    try:
        event = body['event']
        data = event.get('data', {})
        return {
                    'lead_id': event.get('lead_id'),
                    'display_name': data.get('display_name', 'Unknown'),
                    'status_label': data.get('status_label', 'Unknown'),
                    'date_created': data.get('date_created'),
                    'subscription_id': body.get('subscription_id'),
                    'event_id': event.get('id')
                }
    except Exception as e:
        logger.error(f"Extract error: {str(e)}")
        return None


def store_lead_in_s3(lead_data, full_body):
    try:
        lead_id = lead_data['lead_id']
        filename = f"crm_event_{lead_id}.json"
        s3_key = f"{SOURCE_PREFIX}{filename}"
        storage_data = {
                        **full_body,
                        'processed_at': datetime.utcnow().isoformat(),
                        'extracted_lead_data': lead_data
                        }
        s3.put_object(Bucket=BUCKET_NAME, Key=s3_key,
                      Body=json.dumps(storage_data, indent=2),
                      ContentType='application/json',
                        Metadata={'lead_id': lead_id, 'processed_at': datetime.utcnow().isoformat()})        
        return s3_key
    except Exception as e:
        logger.error(f"S3 error: {str(e)}")
        raise


def create_response(status_code, body):
    return {
            'statusCode': status_code,
            'headers': {
                        'Content-Type': 'application/json',
                        'Access-Control-Allow-Origin': '*'
                    },
            'body': json.dumps(body)
        }
