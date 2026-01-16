""" Helper functions for converting string cases """
import re


def dict_camel_to_snake_case(camel_dict):
    """
    Recursively convert camelCased keys for a camelCased dict into snake_cased keys
    """
    converted = {}
    for key, value in camel_dict.items():
        new_value = dict_camel_to_snake_case(value) if isinstance(value, dict) else value
        converted[to_snake_case(key)] = new_value
    return converted


def dict_snake_to_camel_case(snake_dict):
    """
    Recursively convert a snake_cased dict into a camelCased dict
    """
    converted = {}
    for key, value in snake_dict.items():
        new_value = dict_snake_to_camel_case(value) if isinstance(value, dict) else value
        converted[to_camel_case(key)] = new_value
    return converted


def to_camel_case(snake_str):
    """
    Convert a snake_case string into a camelCase string
    """
    components = snake_str.split('_')
    return components[0] + "".join(x.title() for x in components[1:])


def to_snake_case(camel_str):
    """
    Convert a camelCase string into a snake_case string
    """
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', camel_str)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()