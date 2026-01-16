import django_mongoengine_filter
from mongoengine import Q


class MongoDBBaseSearchFilterMixin(django_mongoengine_filter.FilterSet):
    search = django_mongoengine_filter.MethodFilter(action='search_filter')

    class Meta:
        model = None
        fields = ('search',)
        search_fields = ()

    def search_filter(self, queryset, name, value):
        fields = self.Meta.search_fields
        _filter = Q()
        for field in fields:
            _filter |= Q(**{f'{field}__icontains': value})
        queryset = queryset.filter(_filter)
        return queryset