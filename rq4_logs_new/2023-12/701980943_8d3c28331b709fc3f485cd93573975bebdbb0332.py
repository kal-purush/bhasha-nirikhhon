from django.contrib.gis.geos import Point
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
from dump_in.common.response import create_response
from dump_in.photo_booths.selectors.photo_booth_brands import PhotoBoothBrandSelector
from dump_in.photo_booths.selectors.photo_booths import PhotoBoothSelector
from dump_in.photo_booths.serializers import (
    HashtagSerializer,
    PhotoBoothBrandImageSerializer,
)
from dump_in.photo_booths.services import PhotoBoothService
from dump_in.reviews.selectors.reviews import ReviewSelector
from dump_in.reviews.serializers import ConceptSerializer


class PhotoBoothBrandListAPI(APIView):
    authentication_classes = (JWTAuthentication,)
    permission_classes = (IsAuthenticated,)

    class OutputSerializer(BaseSerializer):
        id = serializers.IntegerField()
        name = serializers.CharField()
        logo_image_url = serializers.URLField()

    @swagger_auto_schema(
        tags=["포토부스"],
        operation_summary="포토부스 업체 조회",
        responses={
            status.HTTP_200_OK: BaseResponseSerializer(data_serializer=OutputSerializer),
            status.HTTP_401_UNAUTHORIZED: BaseResponseExceptionSerializer(exception=NotAuthenticated),
        },
    )
    def get(self, request: Request) -> Response:
        """
        인증된 사용자가 포토부스에 업체를 조회합니다.
        url: /app/api/photo-booths/brands
        """
        photo_booth_brand_selector = PhotoBoothBrandSelector()
        photo_booth_brands = photo_booth_brand_selector.get_photo_booth_brand_queryset()
        photo_booth_brands_data = self.OutputSerializer(photo_booth_brands, many=True).data
        return create_response(data=photo_booth_brands_data, status_code=status.HTTP_200_OK)


class PhotoBoothLocationSearchAPI(APIView):
    authentication_classes = (JWTAuthentication,)
    permission_classes = (IsAuthenticated,)

    class FilterSerializer(BaseSerializer):
        photo_booth_brand_name = serializers.CharField(required=True)
        latitude = serializers.FloatField(required=True)
        longitude = serializers.FloatField(required=True)
        radius = serializers.FloatField(max_value=1.5, required=True)

    class OutputSerializer(BaseSerializer):
        id = serializers.UUIDField()
        name = serializers.CharField()
        distance = serializers.SerializerMethodField()

        def get_distance(self, obj) -> str:
            center_point = self.context["center_point"]
            destination_point = obj.point
            distance = center_point.distance(destination_point)
            if distance > 0.01:
                return f"{distance * 100:.2f} km"
            return f"{distance * 100000:.2f} m"

    @swagger_auto_schema(
        tags=["포토부스"],
        operation_summary="포토부스 지점 위치 검색",
        query_serializer=FilterSerializer,
        responses={
            status.HTTP_200_OK: BaseResponseSerializer(data_serializer=OutputSerializer),
            status.HTTP_401_UNAUTHORIZED: BaseResponseExceptionSerializer(exception=NotAuthenticated),
        },
    )
    def get(self, request: Request) -> Response:
        """
        인증된 사용자가 자신의 위를 기준으로 포토부스 지점 위치를 검색합니다. (반경 최대 1.5km)
        url: /app/api/photo-booths/locations/search
        """
        filter_serializer = self.FilterSerializer(data=request.query_params)
        filter_serializer.is_valid(raise_exception=True)
        center_point = Point(
            x=filter_serializer.validated_data["longitude"],
            y=filter_serializer.validated_data["latitude"],
            srid=4326,
        )
        photo_booths_selector = PhotoBoothSelector()
        photo_booths = photo_booths_selector.get_nearby_photo_booth_queryset_by_brand_name(
            center_point=center_point,
            radius=filter_serializer.validated_data["radius"],
            photo_booth_brand_name=filter_serializer.validated_data["photo_booth_brand_name"],
        )
        photo_booths_data = self.OutputSerializer(
            photo_booths,
            many=True,
            context={"center_point": center_point},
        ).data
        return create_response(data=photo_booths_data, status_code=status.HTTP_200_OK)


class PhotoBoothLocationListAPI(APIView):
    authentication_classes = (JWTAuthentication,)
    permission_classes = (IsAuthenticated,)

    class FilterSerializer(BaseSerializer):
        latitude = serializers.FloatField(required=True)
        longitude = serializers.FloatField(required=True)
        radius = serializers.FloatField(max_value=1.5, required=True)

    class OutputSerializer(BaseSerializer):
        id = serializers.UUIDField()
        photo_booth_name = serializers.CharField(source="name")
        photo_booth_brand_name = serializers.CharField(source="photo_booth_brand.name")
        logo_image_url = serializers.URLField(source="photo_booth_brand.logo_image_url")
        latitude = serializers.FloatField()
        longitude = serializers.FloatField()
        is_liked = serializers.BooleanField()
        hashtag = HashtagSerializer(many=True, source="photo_booth_brand.hashtag")

    @swagger_auto_schema(
        tags=["포토부스"],
        operation_summary="포토부스 지점 위치 리스트 조회",
        query_serializer=FilterSerializer,
        responses={
            status.HTTP_200_OK: BaseResponseSerializer(data_serializer=OutputSerializer),
            status.HTTP_401_UNAUTHORIZED: BaseResponseExceptionSerializer(exception=NotAuthenticated),
        },
    )
    def get(self, request: Request) -> Response:
        """
        인증된 사용자가 자신의 위치를 기준으로 포토부스 지점 리스트를 조회합니다. (반경 1.5km)
        url: /app/api/photo-booths/locations
        """
        filter_serializer = self.FilterSerializer(data=request.query_params)
        filter_serializer.is_valid(raise_exception=True)
        center_point = Point(
            x=filter_serializer.validated_data["longitude"],
            y=filter_serializer.validated_data["latitude"],
            srid=4326,
        )
        photo_booths_selector = PhotoBoothSelector()
        photo_booths = photo_booths_selector.get_nearby_photo_booth_queryset_with_brand_and_hashtag_and_user_info(
            center_point=center_point,
            radius=filter_serializer.validated_data["radius"],
            user=request.user,
        )
        photo_booths_data = self.OutputSerializer(
            photo_booths,
            many=True,
        ).data
        return create_response(data=photo_booths_data, status_code=status.HTTP_200_OK)


class PhotoBoothDetailAPI(APIView):
    authentication_classes = (JWTAuthentication,)
    permission_classes = (IsAuthenticated,)

    class FilterSerializer(BaseSerializer):
        latitude = serializers.FloatField(required=True)
        longitude = serializers.FloatField(required=True)

    class OutputSerializer(BaseSerializer):
        id = serializers.UUIDField()
        photo_booth_name = serializers.CharField(source="name")
        photo_booth_brand_name = serializers.CharField(source="photo_booth_brand.name")
        latitude = serializers.FloatField()
        longitude = serializers.FloatField()
        street_address = serializers.CharField()
        road_address = serializers.CharField()
        operation_time = serializers.CharField()
        distance = serializers.SerializerMethodField()
        is_liked = serializers.BooleanField()
        photo_booth_brand_images_url = PhotoBoothBrandImageSerializer(many=True)
        hashtag = HashtagSerializer(many=True, source="photo_booth_brand.hashtag")

        def get_distance(self, obj) -> str:
            center_point = self.context["center_point"]
            destination_point = obj.point
            distance = center_point.distance(destination_point)
            if distance > 0.01:
                return f"{distance * 100:.2f} km"
            return f"{distance * 100000:.2f} m"

    @swagger_auto_schema(
        tags=["포토부스"],
        operation_summary="포토부스 지점 상세 조회",
        query_serializer=FilterSerializer,
        responses={
            status.HTTP_200_OK: BaseResponseSerializer(data_serializer=OutputSerializer),
            status.HTTP_401_UNAUTHORIZED: BaseResponseExceptionSerializer(exception=NotAuthenticated),
        },
    )
    def get(self, request: Request, photo_booth_id: str) -> Response:
        """
        인증된 사용자가 포토부스 지점 상세 정보를 조회합니다.
        url: /app/api/photo-booths/<str:photo_booth_id>
        """
        filter_serializer = self.FilterSerializer(data=request.query_params)
        filter_serializer.is_valid(raise_exception=True)
        center_point = Point(
            x=filter_serializer.validated_data["longitude"],
            y=filter_serializer.validated_data["latitude"],
            srid=4326,
        )
        photo_booth_selector = PhotoBoothSelector()
        photo_booth = photo_booth_selector.get_photo_booth_with_user_info_by_id(
            photo_booth_id=photo_booth_id,
            user=request.user,
        )

        if photo_booth is None:
            raise NotFoundException("Photo Booth does not exist")

        photo_booth_data = self.OutputSerializer(
            photo_booth,
            context={
                "center_point": center_point,
            },
        ).data
        return create_response(data=photo_booth_data, status_code=status.HTTP_200_OK)


class PhotoBoothLikeAPI(APIView):
    authentication_classes = (JWTAuthentication,)
    permission_classes = (IsAuthenticated,)

    class OutputSerializer(BaseSerializer):
        photo_booth_id = serializers.UUIDField()
        is_liked = serializers.BooleanField()

    @swagger_auto_schema(
        tags=["포토부스"],
        operation_summary="포토부스 지점 좋아요",
        responses={
            status.HTTP_200_OK: BaseResponseSerializer(data_serializer=OutputSerializer),
            status.HTTP_401_UNAUTHORIZED: BaseResponseExceptionSerializer(exception=NotAuthenticated),
            status.HTTP_404_NOT_FOUND: BaseResponseExceptionSerializer(exception=NotFoundException),
        },
    )
    def post(self, request: Request, photo_booth_id: str) -> Response:
        """
        인증된 사용자가 포토부스 지점을 좋아요 또는 좋아요 취소합니다.
        url: /app/api/photo-booths/<str:photo_booth_id>/likes
        """
        photo_booth_service = PhotoBoothService()
        photo_booth, is_liked = photo_booth_service.like_photo_booth(photo_booth_id=photo_booth_id, user=request.user)
        photo_booth_data = self.OutputSerializer({"photo_booth_id": photo_booth.id, "is_liked": is_liked}).data
        return create_response(data=photo_booth_data, status_code=status.HTTP_200_OK)


class PhotoBoothReviewListAPI(APIView):
    authentication_classes = (JWTAuthentication,)
    permission_classes = (IsAuthenticated,)

    class FilterSerializer(BaseSerializer):
        limit = serializers.IntegerField(default=15)

    class OutputSerializer(BaseSerializer):
        id = serializers.IntegerField()
        main_thumbnail_image_url = serializers.URLField()
        content = serializers.CharField()
        frame_color = serializers.CharField()
        participants = serializers.IntegerField()
        camera_shot = serializers.CharField()
        goods_amount = serializers.BooleanField()
        curl_amount = serializers.BooleanField()
        concept = ConceptSerializer(many=True)
        created_at = serializers.DateTimeField()
        updated_at = serializers.DateTimeField()

    @swagger_auto_schema(
        tags=["포토부스"],
        operation_summary="포토부스 리뷰 리스트 조회",
        query_serializer=FilterSerializer,
        responses={
            status.HTTP_200_OK: BaseResponseSerializer(data_serializer=OutputSerializer),
            status.HTTP_401_UNAUTHORIZED: BaseResponseExceptionSerializer(exception=NotAuthenticated),
        },
    )
    def get(self, request: Request, photo_booth_id: str) -> Response:
        """
        인증된 사용자가 포토부스 지점 리뷰 리스트를 조회합니다.
        url: /app/api/photo-booths/<str:photo_booth_id>/reviews
        """
        filter_serializer = self.FilterSerializer(data=request.query_params)
        filter_serializer.is_valid(raise_exception=True)
        limit = filter_serializer.validated_data["limit"]
        review_selector = ReviewSelector()
        reviews = review_selector.get_review_queryset_with_review_image_and_concept_by_photo_booth_id(
            photo_booth_id=photo_booth_id
        ).order_by("-created_at")[0:limit]
        reviews_data = self.OutputSerializer(reviews, many=True).data
        return create_response(data=reviews_data, status_code=status.HTTP_200_OK)