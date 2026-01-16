from drf_yasg.utils import swagger_auto_schema
from rest_framework import serializers, status
from rest_framework.exceptions import NotAuthenticated
from rest_framework.permissions import IsAuthenticated
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework_simplejwt.authentication import JWTAuthentication

from dump_in.common.base.serializers import (
    BaseResponseExceptionSerializer,
    BaseResponseSerializer,
    BaseSerializer,
)
from dump_in.common.exception.exceptions import NotFoundException
from dump_in.common.pagination import LimitOffsetPagination, get_paginated_data
from dump_in.common.response import create_response
from dump_in.events.selectors.events import EventSelector
from dump_in.events.serializers import EventImageSerializer
from dump_in.events.services import EventService
from dump_in.photo_booths.serializers import HashtagSerializer


class EventListAPI(APIView):
    authentication_classes = (JWTAuthentication,)
    permission_classes = (IsAuthenticated,)

    class Pagination(LimitOffsetPagination):
        default_limit = 10

    class FilterSerializer(BaseSerializer):
        hashtag = serializers.CharField(required=False)
        limit = serializers.IntegerField(required=False)
        offset = serializers.IntegerField(required=False)

    class OutputSerializer(BaseSerializer):
        id = serializers.IntegerField()
        title = serializers.CharField()
        main_thumbnail_image_url = serializers.URLField()
        start_date = serializers.DateField()
        end_date = serializers.DateField()
        is_liked = serializers.BooleanField()
        photo_booth_brand_name = serializers.CharField(source="photo_booth_brand.name")

    @swagger_auto_schema(
        tags=["이벤트"],
        operation_summary="이벤트 리스트 조회",
        query_serializer=FilterSerializer,
        responses={
            status.HTTP_200_OK: BaseResponseSerializer(data_serializer=OutputSerializer),
            status.HTTP_401_UNAUTHORIZED: BaseResponseExceptionSerializer(exception=NotAuthenticated),
        },
    )
    def get(self, request: Request) -> Response:
        """
        인증된 사용자가 이벤트 리스트를 조회합니다.
        url: /app/api/events/
        """
        filter_serializer = self.FilterSerializer(data=request.query_params)
        filter_serializer.is_valid(raise_exception=True)
        event_selector = EventSelector()
        events = event_selector.get_event_list(
            user=request.user,
            filters=filter_serializer.validated_data,
        )
        pagination_events_data = get_paginated_data(
            pagination_class=self.Pagination,
            serializer_class=self.OutputSerializer,
            queryset=events,
            request=request,
            view=self,
        )
        return create_response(data=pagination_events_data, status_code=status.HTTP_200_OK)


class EventDetailAPI(APIView):
    authentication_classes = (JWTAuthentication,)
    permission_classes = (IsAuthenticated,)

    class OutputSerializer(BaseSerializer):
        id = serializers.IntegerField()
        title = serializers.CharField()
        content = serializers.CharField()
        main_thumbnail_image_url = serializers.URLField()
        start_date = serializers.DateField()
        end_date = serializers.DateField()
        is_liked = serializers.BooleanField()
        hashtag = HashtagSerializer(many=True)
        event_image = EventImageSerializer(many=True)

    @swagger_auto_schema(
        tags=["이벤트"],
        operation_summary="이벤트 상세",
        responses={
            status.HTTP_200_OK: BaseResponseSerializer(data_serializer=OutputSerializer),
            status.HTTP_401_UNAUTHORIZED: BaseResponseExceptionSerializer(exception=NotAuthenticated),
            status.HTTP_404_NOT_FOUND: BaseResponseExceptionSerializer(exception=NotFoundException),
        },
    )
    def get(self, request: Request, event_id: int) -> Response:
        """
        인증된 사용자가 이벤트 상세를 조회합니다.
        url: /app/api/events/<int:event_id>
        """
        event_selector = EventSelector()
        event = event_selector.get_event_with_user_info_by_id(event_id=event_id, user=request.user)

        if event is None:
            raise NotFoundException("Event does not exist.")

        event_data = self.OutputSerializer(event).data
        return create_response(data=event_data, status_code=status.HTTP_200_OK)


class EventLikeAPI(APIView):
    authentication_classes = (JWTAuthentication,)
    permission_classes = (IsAuthenticated,)

    class OutputSerializer(BaseSerializer):
        event_id = serializers.IntegerField()
        is_liked = serializers.BooleanField()

    @swagger_auto_schema(
        tags=["이벤트"],
        operation_summary="이벤트 좋아요",
        responses={
            status.HTTP_200_OK: BaseResponseSerializer(data_serializer=OutputSerializer),
            status.HTTP_401_UNAUTHORIZED: BaseResponseExceptionSerializer(exception=NotAuthenticated),
            status.HTTP_404_NOT_FOUND: BaseResponseExceptionSerializer(exception=NotFoundException),
        },
    )
    def post(self, request: Request, event_id: int) -> Response:
        """
        인증된 사용자가 이벤트를 좋아요 또는 좋아요 취소합니다.
        url: /app/api/events/<int:event_id>/like
        """
        event_service = EventService()
        event, is_liked = event_service.like_event(event_id=event_id, user=request.user)
        event_data = self.OutputSerializer({"event_id": event.id, "is_liked": is_liked}).data
        return create_response(data=event_data, status_code=status.HTTP_200_OK)