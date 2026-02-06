# Automated Lead Assignment and Notification System

## 📋 Overview

This system automates the capture of new leads from Close CRM, enriches them with lead owner information after a delay period, and sends real-time notifications to the sales team via Slack or email.

## 🏗️ Architecture

```
<img width="1044" height="609" alt="image" src="https://github.com/user-attachments/assets/d06f6af7-862d-487e-878a-a5b801698718" />


┌────────────────────────────┐
│   Close CRM (Webhook)      │
│  Event: lead.created       │
└─────────────┬──────────────┘
              │ HTTPS POST
              ▼
┌────────────────────────────┐
│     API Gateway (REST)     │
│   POST /crm/webhook        │
└─────────────┬──────────────┘
              ▼
┌────────────────────────────┐
│ Lambda: Webhook Ingestion  │
│ • Validate payload         │
│ • Normalize lead data      │
│ • Store raw event in S3    │
└─────────────┬──────────────┘
              ▼
┌────────────────────────────┐
│   S3 (Raw Events)          │
│   source/crm_event.json    │
└─────────────┬──────────────┘
              │ S3 Event
              ▼
┌────────────────────────────┐
│ SQS (Delayed Queue)        │
│ Delay: 10 minutes          │
│ Purpose: wait for owner    │
└─────────────┬──────────────┘
              ▼
┌────────────────────────────┐
│ Lambda: Lead Processor     │
│ • Read raw lead from S3    │
│ • Lookup owner (S3)        │
│ • Enrich lead              │
│ • Store enriched output    │
│ • Notify Slack / Email     │
└───────┬───────────┬────────┘
        │           │
        ▼           ▼
┌──────────────┐  ┌──────────────┐
│ S3 (Enriched)│  │ Notifications│
│ target/      │  │ Slack / SNS  │
└──────────────┘  └──────────────┘

```

## 🔄 Data Flow

### 1. **Webhook Reception** (< 1 second)
- Close CRM sends webhook on lead creation
- API Gateway receives POST request
- Lambda validates and processes payload

### 2. **Initial Storage** (< 2 seconds)
- Lead data stored in S3 `source/` folder
- File format: `crm_event_{lead_id}.json`
- S3 event notification sent to SQS

### 3. **Delay Period** (10 minutes)
- SQS holds message for 600 seconds
- Allows CRM time to assign lead owner
- No processing during this time

### 4. **Processing & Enrichment** (5-10 seconds)
- Lambda triggered by SQS
- Fetches lead data from S3
- Looks up owner data from public bucket
- Merges data
- Stores enriched data

### 5. **Notification** (< 3 seconds)
- Formats message
- Sends to Slack and/or Email
- Sales team receives alert

**Total Time**: ~10 minutes from webhook to notification

## 📁 Project Structure

```
crm-lead-system/
├── lambda1/
│   ├── webhook_ingestion.py      # Receives webhooks, stores in S3
|---lambda2
│   ├── lead_processor.py          # Processes delayed leads, sends notifications
|---lambda3
│   ├── notification.py          # Processes delayed leads, sends notifications
├── notification/
│   |__ infrastructure/
|       |__ cloudformation.yaml        # Complete AWS infrastructure
└── README.md                      # This file
└── requirements.txt           # Python dependencies
```

## 🚀 Quick Start

### Prerequisites

- AWS Account with appropriate permissions
- Python 3.11+
- AWS CLI configured
- Slack workspace with webhook URL (optional)
- Email address for notifications (optional)

### Setup Steps

## 📝 Configuration

### Environment Variables

**Webhook Ingestion Lambda:**
- `BUCKET_NAME`: S3 bucket for lead storage
- `SOURCE_PREFIX`: Prefix for raw webhooks (default: `source/`)

**Lead Processor Lambda:**
- `BUCKET_NAME`: S3 bucket for lead storage
- `SOURCE_PREFIX`: Prefix for raw data (default: `source/`)
- `TARGET_PREFIX`: Prefix for enriched data (default: `target/`)
- `LOOKUP_BUCKET`: Public S3 bucket for owner data (default: `dea-lead-owner`)
- `SNS_TOPIC_ARN`: SNS topic for email notifications
- `SLACK_SECRET_NAME`: Secrets Manager secret name for Slack webhook
- `USE_SLACK`: Enable Slack notifications (default: `true`)
- `USE_EMAIL`: Enable email notifications (default: `false`)

### Delay Configuration

Adjust the SQS delay in CloudFormation parameters:

```yaml
Parameters:
  DelaySeconds:
    Type: Number
    Default: 600  # 10 minutes
    MinValue: 0
    MaxValue: 900  # 15 minutes max
```
### Error Handling

The system includes robust error handling:

1. **Webhook Validation**: Invalid payloads return 400 error
2. **Retry Logic**: SQS configured with 3 retry attempts
3. **Dead Letter Queue**: Failed messages moved to DLQ for investigation
4. **Default Values**: Missing owner data uses sensible defaults
5. **Comprehensive Logging**: All errors logged to CloudWatch

## 📊 Success Criteria Checklist

- ✅ **Fast Ingestion**: Leads stored in S3 within 2 seconds of webhook
- ✅ **Delayed Processing**: Processing only starts after 10-minute delay
- ✅ **Accurate Lookup**: Lead owner correctly matched by lead_id
- ✅ **Complete Enrichment**: All required fields present in enriched data
- ✅ **Real-time Notifications**: Team receives alerts within seconds of processing
- ✅ **Parallel Processing**: Multiple leads processed concurrently without conflicts
- ✅ **Error Recovery**: Failed messages automatically retried
- ✅ **Audit Trail**: Complete processing history in CloudWatch logs

---
