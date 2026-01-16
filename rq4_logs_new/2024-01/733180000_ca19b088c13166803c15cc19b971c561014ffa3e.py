from django.utils.decorators import method_decorator
from rest_framework import viewsets, renderers
from rest_framework.decorators import action
from rest_framework.response import Response

from core.Utils.Api.mixins import MappedSerializerVMixin
from .swagger import schemas


@method_decorator(name='render_list', decorator=schemas.LIKED_JOKES_LIST_SCHEMA)
@method_decorator(name='render_items', decorator=schemas.LIKED_JOKES_ITEMS_SCHEMA)
@method_decorator(name='render_retrieve', decorator=schemas.LIKED_JOKES_RETRIEVE_SCHEMA)
class JokesRenderViewSetMixin(MappedSerializerVMixin, viewsets.ReadOnlyModelViewSet):
    @action(detail=False, methods=['get'], url_path='render', url_name='render-list',
            renderer_classes=(renderers.TemplateHTMLRenderer,))
    def render_list(self, request, *args, **kwargs):
        response = self.list(request, *args, **kwargs)
        jokes = response.data.get('results')
        return Response({'jokes': jokes}, template_name='Api/joke_list.html')

    @action(detail=False, methods=['get'], url_path='render-items', url_name='render-items',
            renderer_classes=(renderers.TemplateHTMLRenderer,),
            pagination_class=None, filter_backends=None)
    def render_items(self, request, *args, **kwargs):
        queryset = self.get_queryset()
        try:
            ids = map(lambda x: int(x.strip()), request.GET.get('jokes', '').split(','))
        except (ValueError, TypeError, KeyError):
            ids = []

        jokes = queryset.filter(id__in=ids)
        return Response({'jokes': jokes}, template_name='Api/joke_items.html')

    @action(detail=True, methods=['get'], url_path='render', url_name='render-retrieve',
            renderer_classes=(renderers.TemplateHTMLRenderer,))
    def render_retrieve(self, request, *args, **kwargs):
        response = self.retrieve(request, *args, **kwargs)
        joke = response.data
        return Response({'joke': joke}, template_name='Api/joke_card.html')