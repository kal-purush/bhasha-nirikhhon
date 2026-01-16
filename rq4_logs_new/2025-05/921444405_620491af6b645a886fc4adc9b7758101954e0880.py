from django.http import JsonResponse
from django.shortcuts import render
from rest_framework import viewsets, status

from orders.service.order_service_impl import OrderServiceImpl
from redis_cache.service.redis_cache_service_impl import RedisCacheServiceImpl


class OrderController(viewsets.ViewSet):
    redisCacheService = RedisCacheServiceImpl.getInstance()
    orderService = OrderServiceImpl.getInstance()

    def requestCreateOrder(self, request):
        postRequest = request.data
        items = postRequest.get("items")
        total = postRequest.get("total")
        userToken = postRequest.get("userToken")
        print(f"items: {items}")

        if not userToken:
            return JsonResponse({"error": "userToken이 필요합니다", "success": False}, status=status.HTTP_400_BAD_REQUEST)

        if not items or not isinstance(items, list) or not total:
            return JsonResponse(
                {"error": "items 또는 total 데이터가 올바르지 않습니다.", "success": False},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            accountId = self.redisCacheService.getValueByKey(userToken)

            if not accountId:
                return JsonResponse(
                    {"error": "유효하지 않은 userToken입니다.", "success": False},
                    status=status.HTTP_400_BAD_REQUEST
                )
            orderId = self.orderService.createOrder(accountId, items, total)

            return JsonResponse(
                {"message": "주문이 완료되었습니다.", "orderId": orderId, "success": True},
                status=status.HTTP_200_OK
            )

        except Exception as e:
                print(f"주문 처리 중 오류 발생: {e}")
                return JsonResponse({"error": "서버 내부 오류", "success": False}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


