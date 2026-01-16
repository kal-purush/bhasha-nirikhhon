import uuid

from collections import Iterable
from elemental_core import ElementalBase


class ResourceReferenceKind(ElementalBase):
    """
    Contains static methods for processing, validating, and filtering references
    to Resources.
    """

    @staticmethod
    def process_value(value, **params):
        if not isinstance(value, uuid.UUID):
            value = uuid.UUID(value)
        return value

    @staticmethod
    def validate_value(value, model=None, **params):
        result = True

        view_result_id = params.get('view_result_id')
        if view_result_id:
            view_result = model.retrieve_resource(view_result_id)
            result = value in view_result

        return result

    @staticmethod
    def filter_value(value, **params):
        return True

    @staticmethod
    def sort_values(values, **params):
        return values