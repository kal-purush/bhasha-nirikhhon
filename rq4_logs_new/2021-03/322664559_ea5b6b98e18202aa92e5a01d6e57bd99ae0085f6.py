from rest_framework.pagination import LimitOffsetPagination

class PagePagination(LimitOffsetPagination):
    default_limit = 1
    limit_query_param = 'limit'
    offset_query_param = 'offset'
    max_limit = 20