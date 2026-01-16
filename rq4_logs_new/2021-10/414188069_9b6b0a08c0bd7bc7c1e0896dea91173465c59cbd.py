import json
import logging
import boto3
import os
from urllib.parse import parse_qs
from profiler import profile


logger = logging.getLogger()
logger.setLevel(logging.INFO)


lambda_client = boto3.client('lambda')

db_service = boto3.resource("dynamodb")
client_mapping_table = db_service.Table(os.environ.get("client_mapping_table"))


@profile
def lambda_handler(event, context):
    # Checks the incoming user events and routes them to right handler
    client_id = event.get("pathParameters", {}).get("clientId")
    source = event.get("pathParameters", {}).get("source")
    itsm = event.get("pathParameters", {}).get("itsm")
    interaction = True if event.get("pathParameters", {}).get("interactions") else False
    if interaction:
        logger.info(f"received the interaction payload {event}")
        body = event.get("body")
        json_string = parse_qs(body)["payload"][0]
        print(f"Json String\n{json_string}")
        payload = json.loads(json_string)
    else:
        payload = json.loads(event.get("body"))
    logger.info(f"Incoming Payload: {payload}")
    is_valid_client = check_client(client_id, source, itsm)
    if is_valid_client:
        logger.info(f"Incoming source: {source}")
        data = {
            "client_id": client_id,
            "itsm": itsm,
            "payload": payload
        }
        if source == "slack":
            # Handling Slack Challange verification
            event_type = payload.get("type",  "")

            if event_type == 'url_verification':
                return {'statusCode': 200, 'body': payload['challenge']}

            lambda_client.invoke(FunctionName=os.environ.get("slack_handler_arn"),
                                 InvocationType="Event",
                                 Payload=json.dumps(data))
        elif source == "teams":
            lambda_client.invoke(FunctionName=os.environ.get("teams_handler_arn"),
                                 InvocationType="Event",
                                 Payload=json.dumps(data))
        elif source == "zoom":
            body = json.loads(event.get("body"))
            event_type = body.get("event",  "")
            payload = body.get("payload")
            data = {
                "client_id": client_id,
                "itsm": itsm,
                "payload": payload,
                "type": event_type
            }
            lambda_client.invoke(FunctionName=os.environ.get("zoom_handler_arn"),
                                 InvocationType="Event",
                                 Payload=json.dumps(data))
        message = "Invoked right source handler"
    else:
        message = "Invalid/Inactive client configuration"

    return {
        'statusCode': 200,
        'body': json.dumps(message)
    }


def check_client(client_id, source, itsm):
    """
    Checks if the given client id is valid or not
    """
    logger.info(f"checking the client info for client: {client_id}")
    response = client_mapping_table.get_item(Key={"client_id": client_id})
    logger.debug(f"Response of client_id mapping: {response}")
    if "Item" in response:
        if response.get("Item", {}).get("active"):
            itsms = response.get("Item", {}).get("itsm")
            sources = response.get("Item", {}).get("source")
            if source in sources and itsm in itsms:
                return True
            else:
                logger.error(
                    f"Invalid source: {source} or itsm: {itsm} passed")
        else:
            logger.error("client is Inactive")
    else:
        logger.error("Client not found")