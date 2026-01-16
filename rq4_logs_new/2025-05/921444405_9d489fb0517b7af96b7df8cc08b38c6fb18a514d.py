from django.http import JsonResponse
from rest_framework import viewsets, status
from rest_framework.decorators import action
from rest_framework.response import Response
from django.shortcuts import get_object_or_404

from report.entity.report import Report
from account.entity.account import Account

from report.service.report_service_impl import ReportServiceImpl
from account.service.account_service_impl import AccountServiceImpl
from board.service.board_service_impl import BoardServiceImpl
from comment.service.comment_service_impl import CommentServiceImpl
from redis_cache.service.redis_cache_service_impl import RedisCacheServiceImpl


class ReportController(viewsets.ViewSet):
    __reportService = ReportServiceImpl.getInstance()
    __accountService = AccountServiceImpl.getInstance()
    __boardService = BoardServiceImpl.getInstance()
    __commentService = CommentServiceImpl.getInstance()
    redisCacheService = RedisCacheServiceImpl.getInstance()


    def requestReport(self, request): # 신고 요청
        data = request.data
        user_token = request.headers.get("userToken")
        if not user_token:
            return JsonResponse({"error": "userToken이 필요합니다", "success": False}, status=400)
        account_id = self.redisCacheService.getValueByKey(user_token)

        if not account_id:
            return JsonResponse({"error": "로그인이 필요합니다", "success": False}, status=status.HTTP_401_UNAUTHORIZED)

        target_id = data.get("target_id")
        target_type = data.get("target_type")
        reason_type = data.get("reason_type")

        if not target_id or not target_type or not reason_type:
            return JsonResponse({"error": "필수 항목이 누락되었습니다", "success": False}, status=status.HTTP_400_BAD_REQUEST)

        try:
            account = self.__accountService.findAccountById(account_id)
            if not account:
                raise Exception("계정을 찾을 수 없습니다")

            report = self.__reportService.requestReport(
                reporter=account,
                target_id=target_id,
                target_type=target_type,
                reason_type=reason_type
            )

            return JsonResponse({
                "success": True,
                "message": "신고가 정상적으로 접수되었습니다",
                "report_id": report.id
            }, status=status.HTTP_201_CREATED)

        except Exception as e:
            return JsonResponse({"error": str(e), "success": False}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    #
    # def getReportDetail(self, request, request_id): # 관리자 -신고 상세보기
    #     user_token = request.headers.get("userToken")
    #     if not user_token:
    #         return JsonResponse({"error": "userToken이 필요합니다", "success": False}, status=400)
    #
    #     account_id = self.redisCacheService.getValueByKey(user_token)
    #     if not account_id:
    #         return JsonResponse({"error": "로그인이 필요합니다", "success": False}, status=status.HTTP_401_UNAUTHORIZED)
    #
    #     account = self.__accountService.findAccountById(account_id)
    #     if not account or account.role_type.role_type != 'ADMIN':
    #         return JsonResponse({"error": "관리자 권한이 필요합니다", "success": False}, status=status.HTTP_403_FORBIDDEN)
    #
    #     try:
    #         report = self.__reportService.getReportById(request_id)
    #         report_data = {
    #             "id": report.id,
    #             "reporter_id": report.reporter.id,
    #             "target_id": report.target_id,
    #             "target_type": report.target_type,
    #             "reason_type": report.reason_type,
    #             "created_at": report.created_at,
    #             "processed": report.processed,
    #             "processed_at": report.processed_at,
    #             "processed_by": report.processed_by.id if report.processed_by else None,
    #         }
    #
    #         return JsonResponse({
    #             "success": True,
    #             "report": report_data
    #         }, status=status.HTTP_200_OK)
    #
    #     except Report.DoesNotExist:
    #         return JsonResponse({"error": "해당 신고를 찾을 수 없습니다", "success": False}, status=status.HTTP_404_NOT_FOUND)
    #
    # def getReportsList(self, request):  # 관리자 -신고 리스트 가져오기
    #     user_token = request.headers.get("userToken")
    #     if not user_token:
    #         return JsonResponse({"error": "userToken이 필요합니다", "success": False}, status=400)
    #
    #     account_id = self.redisCacheService.getValueByKey(user_token)
    #     if not account_id:
    #         return JsonResponse({"error": "로그인이 필요합니다", "success": False}, status=status.HTTP_401_UNAUTHORIZED)
    #
    #     account = self.__accountService.findAccountById(account_id)
    #     if not account or account.role_type.role_type != 'ADMIN':  # comment 에서 is_admin 수정하기
    #         return JsonResponse({"error": "관리자 권한이 필요합니다", "success": False}, status=status.HTTP_403_FORBIDDEN)
    #
    #     reports = self.__reportService.getAllReports()
    #     report_list = []
    #
    #     for r in reports:
    #         report_data = {
    #             "id": r.id,
    #             "reporter_id": r.reporter.id,
    #             "target_id": r.target_id,
    #             "target_type": r.target_type,
    #             "reason_type": r.reason_type,
    #             "created_at": r.created_at,
    #             "processed": r.processed,
    #             "processed_at": r.processed_at,
    #             "processed_by": r.processed_by.id if r.processed_by else None,
    #         }
    #         report_list.append(report_data)
    #
    #     return JsonResponse({
    #         "success": True,
    #         "reports": report_list
    #     }, status=status.HTTP_200_OK)
    #
    #
    # def deleteReport(self, request, request_id):  # 관리자 -신고 삭제
    #     user_token = request.headers.get("userToken")
    #     if not user_token:
    #         return JsonResponse({"error": "userToken이 필요합니다", "success": False}, status=400)
    #
    #     account_id = self.redisCacheService.getValueByKey(user_token)
    #     if not account_id:
    #         return JsonResponse({"error": "로그인이 필요합니다", "success": False}, status=status.HTTP_401_UNAUTHORIZED)
    #
    #     account = self.__accountService.findAccountById(account_id)
    #     if not account or account.role_type.role_type != 'ADMIN':
    #         return JsonResponse({"error": "관리자 권한이 필요합니다", "success": False}, status=status.HTTP_403_FORBIDDEN)
    #
    #     try:
    #         self.__reportService.deleteReportById(request_id)
    #
    #         return JsonResponse({
    #             "success": True,
    #             "message": "신고가 성공적으로 삭제되었습니다",
    #             "report_id": request_id
    #         }, status=status.HTTP_200_OK)
    #
    #     except Report.DoesNotExist:
    #         return JsonResponse({"error": "해당 신고를 찾을 수 없습니다", "success": False}, status=status.HTTP_404_NOT_FOUND)
    #
    #     except Exception as e:
    #         return JsonResponse({"error": str(e), "success": False}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    #
    #
    # def updateReportStatus(self, request, request_id):
    #     user_token = request.headers.get("userToken")
    #     if not user_token:
    #         return JsonResponse({"error": "userToken이 필요합니다", "success": False}, status=400)
    #
    #     account_id = self.redisCacheService.getValueByKey(user_token)
    #     if not account_id:
    #         return JsonResponse({"error": "로그인이 필요합니다", "success": False}, status=status.HTTP_401_UNAUTHORIZED)
    #
    #     account = self.__accountService.findAccountById(account_id)
    #     if not account or account.role_type.role_type != 'ADMIN':
    #         return JsonResponse({"error": "관리자 권한이 필요합니다", "success": False}, status=status.HTTP_403_FORBIDDEN)
    #
    #     try:
    #         updated_report = self.__reportService.updateReportStatus(report_id=request_id, admin=account)
    #
    #         return JsonResponse({
    #             "success": True,
    #             "message": "신고가 처리되었습니다",
    #             "report_id": updated_report.id,
    #             "processed_at": updated_report.processed_at,
    #             "processed_by": updated_report.processed_by.id
    #         }, status=status.HTTP_200_OK)
    #
    #     except Report.DoesNotExist:
    #         return JsonResponse({"error": "해당 신고를 찾을 수 없습니다", "success": False}, status=status.HTTP_404_NOT_FOUND)
    #
    #     except Exception as e:
    #         return JsonResponse({"error": str(e), "success": False}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    #