import json
import boto3
import os
import logging
import urllib3
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')
sns = boto3.client('sns')
secrets = boto3.client('secretsmanager')
http = urllib3.PoolManager()

BUCKET_NAME = os.environ.get('BUCKET_NAME')
SOURCE_PREFIX = os.environ.get('SOURCE_PREFIX', 'source/')
TARGET_PREFIX = os.environ.get('TARGET_PREFIX', 'target/')
LOOKUP_BUCKET = os.environ.get('LOOKUP_BUCKET', 'dea-lead-owner')
SNS_TOPIC_ARN = os.environ.get('SNS_TOPIC_ARN', '')
SLACK_SECRET_NAME = os.environ.get('SLACK_SECRET_NAME', '')
USE_SLACK = os.environ.get('USE_SLACK', 'false').lower() == 'true'
USE_EMAIL = os.environ.get('USE_EMAIL', 'false').lower() == 'true'

def lambda_handler(event, context):
    try:
        logger.info(f"Processing {len(event.get('Records', []))} SQS messages")
        
        for record in event.get('Records', []):
            try:
                message_body = json.loads(record['body'])
                if 'Records' in message_body:
                    for s3_record in message_body['Records']:
                        process_lead(s3_record)
            except Exception as e:
                logger.error(f"Error processing record: {str(e)}", exc_info=True)        
            return {'statusCode': 200}
    except Exception as e:
        logger.error(f"Handler error: {str(e)}", exc_info=True)
        raise

def process_lead(s3_record):
    bucket = s3_record['s3']['bucket']['name']
    key = s3_record['s3']['object']['key']
    
    logger.info(f"Processing: s3://{bucket}/{key}")
    
    # Download lead data
    response = s3.get_object(Bucket=bucket, Key=key)
    lead_data = json.loads(response['Body'].read().decode('utf-8'))
    
    # Extract lead_id
    lead_id = lead_data.get('event', {}).get('lead_id')
    if not lead_id:
        logger.error("No lead_id found")
        return
    
    # Lookup owner data
    owner_data = lookup_lead_owner(lead_id)
    
    # Enrich data
    enriched = enrich_lead_data(lead_data, owner_data)
    
    # Store enriched data
    store_enriched_data(enriched, lead_id)
    
    # Send notifications
    send_notifications(enriched)

def lookup_lead_owner(lead_id):
    try:
        url = f"https://{LOOKUP_BUCKET}.s3.us-east-1.amazonaws.com/{lead_id}.json"
        logger.info(f"Looking up: {url}")
        
        response = http.request('GET', url)
        if response.status == 200:
            return json.loads(response.data.decode('utf-8'))
        logger.warning(f"Lookup failed: HTTP {response.status}")
        return None
    except Exception as e:
        logger.error(f"Lookup error: {str(e)}")
        return None

def enrich_lead_data(lead_data, owner_data):
    event_data = lead_data.get('event', {}).get('data', {})
    extracted = lead_data.get('extracted_lead_data', {})
    
    if not owner_data:
        owner_data = {
                        'lead_email': 'not-available@example.com',
                        'lead_owner': 'Unassigned',
                        'funnel': 'Unknown'
                                }
    
        return {
            'lead_id': owner_data.get('lead_id', extracted.get('lead_id')),
            'display_name': event_data.get('display_name', extracted.get('display_name', 'Unknown')),
            'status_label': event_data.get('status_label', extracted.get('status_label', 'Unknown')),
            'date_created': event_data.get('date_created', extracted.get('date_created')),
            'lead_email': owner_data.get('lead_email', 'N/A'),
            'lead_owner': owner_data.get('lead_owner', 'Unassigned'),
            'funnel': owner_data.get('funnel', 'Unknown'),
            'enriched_at': datetime.utcnow().isoformat()
        }

def store_enriched_data(enriched, lead_id):
    s3_key = f"{TARGET_PREFIX}enriched_{lead_id}.json"
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=s3_key,
        Body=json.dumps(enriched, indent=2),
        ContentType='application/json'
    )
    logger.info(f"Stored: s3://{BUCKET_NAME}/{s3_key}")

def send_notifications(enriched):
    if USE_SLACK:
        send_slack(enriched)
    if USE_EMAIL and SNS_TOPIC_ARN:
        send_email(enriched)

def send_slack(enriched):
    try:
        if not SLACK_SECRET_NAME:
            return
        
        secret = secrets.get_secret_value(SecretId=SLACK_SECRET_NAME)
        webhook_url = json.loads(secret['SecretString'])['webhook_url']
        
        message = {
                    "text": "🎯 New Lead Alert",
                    "blocks": [{
                        "type": "section",
                        "fields": [
                    {"type": "mrkdwn", "text": f"*Name:*\n{enriched['display_name']}"},
                    {"type": "mrkdwn", "text": f"*Lead ID:*\n{enriched['lead_id']}"},
                    {"type": "mrkdwn", "text": f"*Email:*\n{enriched['lead_email']}"},
                    {"type": "mrkdwn", "text": f"*Owner:*\n{enriched['lead_owner']}"},
                    {"type": "mrkdwn", "text": f"*Funnel:*\n{enriched['funnel']}"},
                    {"type": "mrkdwn", "text": f"*Status:*\n{enriched['status_label']}"}
                        ]
                    }]
                }
        
        http.request('POST', webhook_url, body=json.dumps(message).encode('utf-8'), headers={'Content-Type': 'application/json'})
        logger.info("Slack notification sent")
    except Exception as e:
        logger.error(f"Slack error: {str(e)}")

def send_email(enriched):
    try:
        message = f"""New Lead Alert
            Name: {enriched['display_name']}
            Lead ID: {enriched['lead_id']}
            Created: {enriched['date_created']}
            Status: {enriched['status_label']}

            Email: {enriched['lead_email']}
            Lead Owner: {enriched['lead_owner']}
            Funnel: {enriched['funnel']}
            """
        sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=f"🎯 New Lead: {enriched['display_name']}",
        Message=message
                )
        logger.info("Email notification sent")
    except Exception as e:
        logger.error(f"Email error: {str(e)}")
        