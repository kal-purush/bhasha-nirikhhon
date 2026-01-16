from rest_framework import viewsets
from rest_framework.pagination import PageNumberPagination
from rest_framework.permissions import IsAuthenticated

from course_discovery.apps.api import serializers
from course_discovery.apps.course_metadata.models import ProgramEliteExtend


class ProgramEliteExtendViewSet(viewsets.ReadOnlyModelViewSet):
    """ ProgramExtend resource. """
    lookup_field = 'program'
    pagination_class = PageNumberPagination
    permission_classes = (IsAuthenticated,)
    serializer_class = serializers.ProgramEliteExtendSerializer

    def get_queryset(self):
        return serializers.ProgramEliteExtendSerializer.prefetch_queryset()

    def list(self, request, *args, **kwargs):
        """ Retrieve a list of all program extend. """
        return super(ProgramEliteExtendViewSet, self).list(request, *args, **kwargs)

    def retrieve(self, request, *args, **kwargs):
        """ Retrieve details for a person. """
        return super(ProgramEliteExtendViewSet, self).retrieve(request, *args, **kwargs)