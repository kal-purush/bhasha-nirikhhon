import json
import sys

from jsonschema import validate
from jsonschema.exceptions import ValidationError

schema_AWS_IAM_Role_Policy = {
    "type": "object",
    "properties": {
        "PolicyName": {
            "type": "string"
        },
        "PolicyDocument": {
            "type": "object",
            "properties": {
                "Version": {
                    "type": "string"
                },
                "Statement": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "Sid": {
                                "type": "string"
                            },
                            "Effect": {
                                "type": "string",
                                "enum": ["Allow", "Deny"]
                            },
                            "Action": {
                                "oneOf": [
                                    {
                                        "type": "array",
                                        "items": {
                                            "type": "string"
                                        }
                                    },
                                    {
                                        "type": "string",
                                    }
                                ]
                            },
                            "Resource": {
                                "oneOf": [
                                    {
                                        "type": "array",
                                        "items": {
                                            "type": "string"
                                        }
                                    },
                                    {
                                        "type": "string",

                                    }
                                ]
                            },
                            "Condition": {
                                "type": "object"
                            }

                        },
                        "required": ["Effect", "Action", "Resource"],
                        "additionalProperties": False
                    }

                }

            },
            "required": ["Version", "Statement"],
            "additionalProperties": False
        }
    },
    "required": ["PolicyName", "PolicyDocument"],
    "additionalProperties": False
}


def load_json_file(path_to_file):
    try:
        with open(path_to_file, 'r') as file:
            data = json.load(file)
        validate(data, schema_AWS_IAM_Role_Policy)
        return data
    except FileNotFoundError:
        raise FileNotFoundError(f"File not found: {path_to_file}")
    except json.JSONDecodeError as e:
        raise Exception(f"Error decoding JSON from {path_to_file}: {str(e)}") from e
    except ValidationError as e:
        raise ValidationError(f"The data provided is not in the AWS::IAM::Role Policy format: {e.message}")


def check_for_asterisk(path_to_file):
    data = load_json_file(path_to_file)
    statements = data.get('PolicyDocument', {}).get('Statement', [])
    for statement in statements:
        resources = statement.get('Resource', [])
        if not isinstance(resources, list) and resources == '*':
            return False
    return True


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python <script.py> <path_to_json_file>")
        sys.exit(1)
    file_path = sys.argv[1]
    result = check_for_asterisk(file_path)
    print(result)