import json
import time

from base.lambda_handler_base import APILambdaHandlerBase
from base.api_exceptions import BadRequestException, UnauthorizedException

TABLE_NAME = 'salt_level'
LOCAL_TABLE_NAME = 'salt_level_local'

class SaltLevelLambdaHandler(APILambdaHandlerBase):
    auth_actions = {
        'create': True,
        'list': False,
    }

    primary_partition_key = 'water_softener_id'

    @property
    def require_auth(self):
        return self.auth_actions[self.action]

    def _init(self):
        self.table_name = LOCAL_TABLE_NAME if self.is_local else TABLE_NAME
        self.actions = {
            'create': self._create,
            'list': self._list,
        }
        self.validation_actions = {
            'create': self._validate_create,
        }

    def _init_aws(self):
        self.ddb_client = self.aws_session.client('dynamodb', region_name='us-east-1')

    def _parse_payload(self, payload):
        self.action = payload.get('action')
        if not self.action:
            raise BadRequestException('action parameter required')
        self.action = self.action.lower()
        if self.action not in self.actions:
            raise BadRequestException('invalid action')
        self.payload = payload.get('payload')

    def _validate_create(self):
        self.water_softener_id = self.payload.get('water_softener_id')
        if self.water_softener_id is None:
            raise BadRequestException('water_softener_id parameter required')

        self.sensor_one = self.payload.get('sensor_one')
        if self.sensor_one is None:
            raise BadRequestException('sensor_one parameter required')

    def _build_key(self, water_softener_id):
        return {
            'water_softener_id': {
                'S': water_softener_id,
            },
        }

    def _build_new_ddb_item(self, water_softener_id, sensor_one):
        return {
            'water_softener_id': {
                'S': water_softener_id,
            },
            'sensor_one': {
                'N': str(sensor_one),
            },
            'timestamp': {
                'N': str(int(time.time())),
            }
        }

    def _create(self):
        print("New entry for: {}".format(self.water_softener_id))
        ddbItem = self._build_new_ddb_item(self.water_softener_id, self.sensor_one)
        self.ddb_client.put_item(
            TableName=self.table_name,
            Item=ddbItem,
        )
        return {
            'water_softener_id': ddbItem['water_softener_id'],
            'sensor_one': ddbItem['sensor_one'],
            'timestamp': ddbItem['timestamp'],
        }

    def _list(self):
        ddb_items = self.ddb_client.scan(
            TableName=self.table_name,
        )['Items']
        items = [
            {
                key: value[list(value.keys())[0]]
                for key, value in ddb_item.items()
            } for ddb_item in ddb_items
        ]
        return items

    def _run(self):
        result = self.actions[self.action]()

        return {
            "isBase64Encoded": False,
            "statusCode": 200,
            "headers": {
                "Access-Control-Allow-Origin": "*"
            },
            "multiValueHeaders": {},
            "body": json.dumps(result)
        }


def lambda_handler(event, context):
    return SaltLevelLambdaHandler(event, context).handle()