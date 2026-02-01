"""
Unit Tests for Webhook Ingestion Handler
"""

import json
import pytest
import os
import sys
from unittest.mock import patch, MagicMock

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../lambda'))

from webhook_ingestion import (
    lambda_handler,
    validate_webhook,
    extract_lead_data,
    create_response
)


class TestWebhookIngestion:
    
    @pytest.fixture
    def sample_webhook_event(self):
        """Sample webhook event for testing"""
        return {
            'body': json.dumps({
                "subscription_id": "whsub_test",
                "event": {
                    "id": "ev_test",
                    "lead_id": "lead_test123",
                    "action": "created",
                    "data": {
                        "display_name": "Test Lead",
                        "status_label": "Potential",
                        "date_created": "2025-01-28T12:00:00+00:00",
                        "contact_ids": ["cont_test"]
                    }
                }
            })
        }
    
    @pytest.fixture
    def invalid_webhook_event(self):
        """Invalid webhook event missing required fields"""
        return {
            'body': json.dumps({
                "subscription_id": "whsub_test"
                # Missing 'event' field
            })
        }
    
    def test_validate_webhook_success(self, sample_webhook_event):
        """Test successful webhook validation"""
        body = json.loads(sample_webhook_event['body'])
        assert validate_webhook(body) is True
    
    def test_validate_webhook_missing_event(self, invalid_webhook_event):
        """Test validation fails when 'event' is missing"""
        body = json.loads(invalid_webhook_event['body'])
        assert validate_webhook(body) is False
    
    def test_validate_webhook_wrong_action(self):
        """Test validation ignores non-created events"""
        body = {
            "event": {
                "lead_id": "lead_test",
                "action": "updated",  # Not 'created'
                "data": {}
            }
        }
        assert validate_webhook(body) is False
    
    def test_extract_lead_data_success(self, sample_webhook_event):
        """Test successful lead data extraction"""
        body = json.loads(sample_webhook_event['body'])
        lead_data = extract_lead_data(body)
        
        assert lead_data is not None
        assert lead_data['lead_id'] == 'lead_test123'
        assert lead_data['display_name'] == 'Test Lead'
        assert lead_data['status_label'] == 'Potential'
    
    def test_extract_lead_data_with_custom_fields(self):
        """Test extraction of custom fields"""
        body = {
            "event": {
                "lead_id": "lead_test",
                "data": {
                    "display_name": "Test",
                    "custom.cf_am3UgCUhyM5iNDtAPL84enDjUrZx1JsyVZ9uD9TbYwG": "Test Funnel"
                }
            }
        }
        
        lead_data = extract_lead_data(body)
        assert lead_data['custom_fields']['funnel'] == 'Test Funnel'
    
    def test_create_response_success(self):
        """Test response creation with 200 status"""
        response = create_response(200, {"message": "Success"})
        
        assert response['statusCode'] == 200
        assert 'Content-Type' in response['headers']
        assert json.loads(response['body'])['message'] == 'Success'
    
    def test_create_response_error(self):
        """Test response creation with error status"""
        response = create_response(400, {"error": "Bad Request"})
        
        assert response['statusCode'] == 400
        assert json.loads(response['body'])['error'] == 'Bad Request'
    
    @patch('webhook_ingestion.s3')
    def test_lambda_handler_success(self, mock_s3, sample_webhook_event):
        """Test full lambda handler execution"""
        os.environ['BUCKET_NAME'] = 'test-bucket'
        
        response = lambda_handler(sample_webhook_event, None)
        
        assert response['statusCode'] == 200
        assert mock_s3.put_object.called
        
        # Verify S3 put_object was called with correct parameters
        call_args = mock_s3.put_object.call_args
        assert call_args[1]['Bucket'] == 'test-bucket'
        assert 'crm_event_lead_test123.json' in call_args[1]['Key']
    
    @patch('webhook_ingestion.s3')
    def test_lambda_handler_invalid_payload(self, mock_s3, invalid_webhook_event):
        """Test lambda handler with invalid payload"""
        response = lambda_handler(invalid_webhook_event, None)
        
        assert response['statusCode'] == 400
        assert mock_s3.put_object.called is False
    
    @patch('webhook_ingestion.s3')
    def test_lambda_handler_s3_error(self, mock_s3, sample_webhook_event):
        """Test lambda handler when S3 operation fails"""
        mock_s3.put_object.side_effect = Exception("S3 Error")
        
        response = lambda_handler(sample_webhook_event, None)
        
        assert response['statusCode'] == 500


if __name__ == '__main__':
    pytest.main([__file__, '-v'])