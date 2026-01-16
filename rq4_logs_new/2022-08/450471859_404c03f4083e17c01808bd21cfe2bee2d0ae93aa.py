"""
search_service/serializers/fields.py - Fields for search service types and relations. 
"""
import logging

from rest_framework import serializers

from ..models import (
    Namespace,
)

logger = logging.getLogger(__name__)


class NamespacesField(serializers.SlugRelatedField):
    """Namespaces on an Indexable or Resource are serialized
    out to a list of urns, and in to a list of Namespace objects.
    When used as a writable serializer will carry out a get_or_create
    using the urns provided.
    """

    queryset = Namespace.objects.all()

    def to_internal_value(self, data):
        queryset = self.get_queryset()
        try:
            return queryset.get_or_create(**{self.slug_field: data})[0]
        except (TypeError, ValueError):
            self.fail("invalid")