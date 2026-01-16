import json
import boto3

def lambda_handler(event, context):
    runtime = boto3.client("sagemaker-runtime")
    
    # Make sure that the training_feature_names.csv file is located at the root of the Lambda folder
    with open('training_feature_names.csv', mode="r", encoding="utf-8") as csv_file:
        response = runtime.invoke_endpoint(
            # Replace the following value with your deployed Endpoint name
            EndpointName="sagemaker-scikit-learn-2023-06-05-07-35-00-839",
            Body=csv_file.read().encode('utf-8'),
            ContentType="text/csv",
        )
        
        return {
            'statusCode': 200,
            'body': json.dumps(json.loads(response["Body"].read().decode('utf-8')))
        }