from rest_framework import viewsets, status
from rest_framework.response import Response
from subscribe.service.subscribe_service_impl import SubscribeServiceImpl

class SubscribeController(viewsets.ViewSet):
    subscribeService = SubscribeServiceImpl.getInstance()

    def requestSubscribeList(self, request):
        try:
            subscribe_list = self.subscribeService.getAllSubscribes()
            result = [
                {
                    "id": s.id,
                    "name": s.name,
                    "price": s.price,
                    "plan_type": s.plan_type,
                    "duration_days": s.duration_days,
                    "is_active": s.is_active,
                }
                for s in subscribe_list
            ]
            return Response(result, status=status.HTTP_200_OK)
        except Exception as e:
            return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    #  구독 생성
    def requestCreateSubscribe(self, request):
        subscriber_id = request.data.get("subscriberId")
        subscribe_id = request.data.get("subscribeId")

        if not subscriber_id or not subscribe_id:
            return Response({"error": "subscriberId, subscribeId는 필수입니다."}, status=status.HTTP_400_BAD_REQUEST)

        try:
            account_subscribe = self.subscribeService.createSubscribe(subscriber_id, subscribe_id)
            return Response({"message": "구독이 생성되었습니다.", "subscribeId": account_subscribe.id},
                            status=status.HTTP_201_CREATED)
        except Exception as e:
            return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    #  유저 구독 상태 조회
    def requestGetSubscriber(self, request):
        subscriber_id = request.query_params.get("subscriberId")

        if not subscriber_id:
            return Response({"error": "subscriberId는 필수입니다."}, status=status.HTTP_400_BAD_REQUEST)

        try:
            subscriber_status = self.subscribeService.getSubscriberStatus(subscriber_id)

            if subscriber_status.get("is_subscribed") is False:
                return Response(status=status.HTTP_204_NO_CONTENT)

            return Response(subscriber_status, status=status.HTTP_200_OK)

        except Exception as e:
            return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    #  유저 구독 히스토리 조회
    def requestGetSubscriberHistory(self, request):
        subscriber_id = request.query_params.get("subscriberId")
        if not subscriber_id:
            return Response({"error": "subscriberId는 필수입니다."}, status=status.HTTP_400_BAD_REQUEST)

        try:
            history = self.subscribeService.getSubscriberHistory(subscriber_id)
            return Response({"history": history}, status=status.HTTP_200_OK)
        except Exception as e:
            return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    #  자동 갱신 처리 (만료 예정 구독 자동 갱신)
    def requestRenewScheduledSubscriptions(self, request):
        try:
            renewed_count = self.subscribeService.renewScheduledSubscriptions()
            return Response({"message": f"자동 갱신 완료: {renewed_count}건 갱신됨"}, status=status.HTTP_200_OK)
        except Exception as e:
            return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    #  만료된 구독 비활성화
    def requestDeactivateExpiredSubscriptions(self, request):
        try:
            deactivated_count = self.subscribeService.deactivateExpiredSubscriptions()
            return Response({"message": f"만료된 구독 {deactivated_count}건 비활성화됨"}, status=status.HTTP_200_OK)
        except Exception as e:
            return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    #  구독 취소
    def requestCancelSubscribe(self, request):
        subscriber_id = request.data.get("subscriberId")
        if not subscriber_id:
            return Response({"error": "subscriberId는 필수입니다."}, status=status.HTTP_400_BAD_REQUEST)

        try:
            self.subscribeService.cancelSubscribe(subscriber_id)
            return Response({"message": "구독이 취소되었습니다."}, status=status.HTTP_200_OK)
        except Exception as e:
            return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
