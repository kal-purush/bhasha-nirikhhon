import json
from decimal import Decimal
from json import JSONDecoder, JSONEncoder


def dumps(obj):
    return json.dumps(obj, cls=DynamoDBEncoder)


class DynamoDBModel(object):
    def to_json(self):
        return self.__dict__


class DynamoDBEncoder(JSONEncoder):
    def default(self, obj):
        if hasattr(obj, 'to_json'):
            return obj.to_json()
        elif isinstance(obj, Decimal):
            return float(obj) if obj % 1 > 0 else int(obj)
        else:
            return JSONEncoder.default(self, obj)


class DynamoDBDecode(JSONDecoder):
    pass