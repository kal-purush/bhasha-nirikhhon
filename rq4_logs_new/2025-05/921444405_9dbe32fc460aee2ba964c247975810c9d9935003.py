from django.http import JsonResponse
from rest_framework import viewsets, status
from payments.service.payments_service_impl import PaymentsServiceImpl
from redis_cache.service.redis_cache_service_impl import RedisCacheServiceImpl

class PaymentsController(viewsets.ViewSet):
    redisCacheService = RedisCacheServiceImpl.getInstance()
    paymentsService = PaymentsServiceImpl.getInstance()

    def requestProcessPayments(self, request):
        postRequest = request.data
        userToken = postRequest.get("userToken")

        if not userToken:
            return JsonResponse({"error": "userToken이 필요합니다", "success": False}, status=status.HTTP_400_BAD_REQUEST)

        try:
            accountId = self.redisCacheService.getValueByKey(userToken)
            if not accountId:
                return JsonResponse({"error": "유효하지 않은 userToken입니다", "success": False}, status=status.HTTP_401_UNAUTHORIZED)

            paymentKey = postRequest.get("paymentKey")
            orderId = postRequest.get("orderId")
            amount = postRequest.get("amount")
            orderInfoId = postRequest.get("orderInfoId")

            if not paymentKey or not orderId or not amount or not orderInfoId:
                return JsonResponse(
                    {"error": "paymentKey, orderId, amount는 필수입니다.", "success": False},
                    status=status.HTTP_400_BAD_REQUEST,
                )

            paymentResult = self.paymentsService.process(accountId, paymentKey, orderId, amount, orderInfoId)
            if paymentResult and isinstance(paymentResult, dict):
                return JsonResponse({
                    "success": True,
                    "message": "결제가 성공적으로 처리되었습니다.",
                    "approvedAt": paymentResult.get("approvedAt"),
                    "orderName": paymentResult.get("orderName"),
                    "method": paymentResult.get("method"),
                    "receipt_url": paymentResult.get("receipt", {}).get("url"),
                    "orderId": orderId,
                    "planName": paymentResult.get("planName"),
                    "amountWithCurrency": f"{paymentResult.get('easyPay', {}).get('amount')} {paymentResult.get('currency', 'KRW')}",
                }, status=status.HTTP_200_OK)
            else:
                return JsonResponse({"error": "결제 처리 중 오류 발생", "success": False}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        except Exception as e:
            return JsonResponse({"error": "서버 내부 오류", "success": False}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)