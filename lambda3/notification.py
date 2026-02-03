import boto3
import cfnresponse
import json

s3 = boto3.client('s3')

def handler(event, context):
    try:
        print(f"Event: {json.dumps(event)}")
        bucket = event['ResourceProperties']['BucketName']
        queue_arn = event['ResourceProperties']['QueueArn']
        prefix = event['ResourceProperties']['Prefix']
        
        if event['RequestType'] == 'Delete':
            print(f"Delete request - removing notification for {bucket}")
            try:
                # Get current config
                current = s3.get_bucket_notification_configuration(Bucket=bucket)
                # Remove our queue configuration
                queue_configs = [q for q in current.get('QueueConfigurations', []) 
                                if q.get('QueueArn') != queue_arn]
                          
                # Update with remaining configs
                new_config = {}
                if queue_configs:
                    new_config['QueueConfigurations'] = queue_configs
      
                s3.put_bucket_notification_configuration(
                    Bucket=bucket,
                    NotificationConfiguration=new_config
                )
            except Exception as e:
                print(f"Error during delete: {str(e)}")
  
            cfnresponse.send(event, context, cfnresponse.SUCCESS, {})
            return
        
        # Create or Update
        print(f"Configuring S3 notification for bucket: {bucket}")
        
        # Get existing configuration
        try:
            existing_config = s3.get_bucket_notification_configuration(Bucket=bucket)
            print(f"Existing config: {json.dumps(existing_config)}")
        except Exception as e:
            print(f"No existing config: {str(e)}")
            existing_config = {}
        
        # Build new queue configuration
        new_queue_config = {
            'QueueArn': queue_arn,
            'Events': ['s3:ObjectCreated:*'],
            'Filter': {
                'Key': {
                    'FilterRules': [{'Name': 'prefix', 'Value': prefix}]
                }
            }
        }
        
        # Merge with existing configurations
        queue_configs = existing_config.get('QueueConfigurations', [])
        
        # Remove any existing config for the same queue
        queue_configs = [q for q in queue_configs if q.get('QueueArn') != queue_arn]
        
        # Add our new config
        queue_configs.append(new_queue_config)
        
        # Build final configuration
        notification_config = {'QueueConfigurations': queue_configs}
        
        # Preserve other notification types if they exist
        if 'TopicConfigurations' in existing_config:
            notification_config['TopicConfigurations'] = existing_config['TopicConfigurations']
        if 'LambdaFunctionConfigurations' in existing_config:
            notification_config['LambdaFunctionConfigurations'] = existing_config['LambdaFunctionConfigurations']
        
        print(f"Applying notification config: {json.dumps(notification_config)}")
        
        s3.put_bucket_notification_configuration(
            Bucket=bucket,
            NotificationConfiguration=notification_config
        )
        
        print("Successfully configured S3 notification")
        cfnresponse.send(event, context, cfnresponse.SUCCESS, {
            'Message': f'S3 notification configured for {bucket}'
        })
        
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
        cfnresponse.send(event, context, cfnresponse.FAILED, {
            'Message': str(e)
        })
