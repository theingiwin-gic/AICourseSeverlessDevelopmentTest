import json
import os
import boto3
from botocore.exceptions import BotoCoreError, ClientError
from boto3.dynamodb.conditions import Key

# DynamoDB client (ENV BASED — exactly as requested)
TABLE_NAME = os.environ["TABLE_NAME"]
AWS_REGION = os.environ["AWS_REGION"]

dynamodb = boto3.resource(
    "dynamodb",
    region_name=AWS_REGION
)

table = dynamodb.Table(TABLE_NAME)


def lambda_handler(event, context):

    try:
        action = event.get("action")

        # WRITE
        if action == "write":
            item = {
                "id": event["id"],
                "created_at": event["created_at"],
                "name": event["name"],
                "age": event["age"]
            }
            table.put_item(Item=item)
            return {
                "message": "Item written successfully"
            }

        # READ
        elif action == "read":
            response = table.get_item(
                Key={
                    "id": event["id"],
                    "created_at": event["created_at"]
                }
            )
            item = response.get("Item", {})
            return {
                "message": "Item retrieved successfully",
                "data": item
            }

        # QUERY (FIXED, logic preserved)
        elif action == "query":
            response = table.query(
                KeyConditionExpression=
                    Key("id").eq(event["id"]) &
                    Key("created_at").gte(event.get("start_time", "0"))
            )
            items = response.get("Items", [])
            return {
                "data": items
            }

        # SCAN
        elif action == "scan":
            response = table.scan()
            items = response.get("Items", [])
            return {
                "data": items
            }

        else:
            return {
                "message": "Invalid action"
            }

    except (BotoCoreError, ClientError) as e:
        return {
            "error": str(e)
        }
