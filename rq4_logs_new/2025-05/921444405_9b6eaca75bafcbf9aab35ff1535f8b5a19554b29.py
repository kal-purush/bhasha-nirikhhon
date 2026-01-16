from django.http import JsonResponse
from rest_framework import viewsets, status
from datetime import datetime, timedelta
from account.service.account_service_impl import AccountServiceImpl
from admin_user_ban.service.admin_user_ban_service_impl import AdminUserBanServiceImpl
from redis_cache.service.redis_cache_service_impl import RedisCacheServiceImpl
from rest_framework.response import Response


class AdminUserBanController(viewsets.ViewSet):
    __accountService = AccountServiceImpl.getInstance()
    __adminUserBanService = AdminUserBanServiceImpl.getInstance()
    redisCacheService = RedisCacheServiceImpl.getInstance()


    # 이메일 복호화
    def __decryptEmail(self, account):
        try:
            decrypted_email = account.get_decrypted_email()
            print(f"복호화된 이메일: {decrypted_email}")
            return decrypted_email
        except Exception as e:
            print(f"[ERROR] 이메일 복호화 실패: {str(e)}")
            return account.email  # fallback: 암호화 된 그대로 반환

    # 관리자 로그인, 권한 확인
    def __checkAdminBanPermission(self, user_token):
        # 유저 토큰 확인
        if not user_token:
            return None, JsonResponse({"error": "userToken이 필요합니다", "success": False},
                                      status=status.HTTP_400_BAD_REQUEST)

        # 관리자 계정 로그인 확인
        admin_account_id = self.redisCacheService.getValueByKey(user_token)
        if not admin_account_id:
            return None, JsonResponse({"error": "로그인이 필요합니다.", "success": False}, status=status.HTTP_401_UNAUTHORIZED)

        # 관리자 권한 확인
        admin_account = self.__accountService.findAccountById(admin_account_id)
        if not admin_account or admin_account.role_type.role_type != 'ADMIN':
            return None, JsonResponse({"error": "관리자 권한이 필요합니다.", "success": False}, status=status.HTTP_403_FORBIDDEN)

        return admin_account, None

    # 관리자 -사용자 계정 차단 (BAN, 영구 탈퇴)
    def banAccount(self, request):
        user_token = request.headers.get("userToken")
        target_account_id = request.data.get("target_account_id")
        reason = request.data.get("reason", "차단 사유")

        admin_account, error_response = self.__checkAdminBanPermission(user_token)
        if error_response:
            return error_response

        if not target_account_id:
            return JsonResponse({"error": "target_account_id가 필요합니다", "success": False},
                                status=status.HTTP_400_BAD_REQUEST)

        # 대상 사용자 계정 확인
        target_account = self.__accountService.findAccountById(target_account_id)
        if not target_account:
            return JsonResponse({"error": "대상 사용자를 찾을 수 없습니다.", "success": False}, status=status.HTTP_404_NOT_FOUND)

        # 영구탈퇴 여부 확인
        if target_account.account_status == 4:
            return JsonResponse({"error": "이미 영구탈퇴 된 사용자 입니다.", "success": False},
                                status=status.HTTP_400_BAD_REQUEST)

        try:
            banned_account = self.__adminUserBanService.banAccountById(target_account_id, reason)
            decrypted_email = self.__decryptEmail(banned_account)
            return Response({
                "success": True,
                "message": f"사용자 {decrypted_email} (ID: {banned_account.id})이 차단되었습니다.",
                "reason": banned_account.suspension_reason
            }, status=status.HTTP_200_OK)
        except Exception as e:
            return Response({"error": str(e), "success": False}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    # 관리자 -차단 사용자 차단 해제
    def unbanAccount(self, request, account_id):  # URL에서 account_id 직접 받기
        user_token = request.headers.get("userToken")

        admin_account, error_response = self.__checkAdminBanPermission(user_token)
        if error_response:
            return error_response

        # 대상 사용자 확인
        target_account = self.__accountService.findAccountById(account_id)  # URL에서 받아온 account_id 사용
        if not target_account:
            return JsonResponse({"error": "대상 사용자를 찾을 수 없습니다.", "success": False}, status=status.HTTP_404_NOT_FOUND)

        # 영구 탈퇴 여부 확인
        if target_account.account_status != 4:
            return JsonResponse({"error": "대상 사용자가 영구탈퇴 된 상태가 아닙니다.", "success": False},
                                status=status.HTTP_400_BAD_REQUEST)
        try:
            self.__adminUserBanService.unbanAccountById(account_id)
            return Response({"success": True, "message": "사용자 계정의 영구 탈퇴가 해제되었습니다."}, status=status.HTTP_200_OK)
        except ValueError as e:
            return Response({"error": str(e)}, status=status.HTTP_400_BAD_REQUEST)

    # 관리자 -차단 사용자 목록 요청
    def getBannedAccounts(self, request):
        user_token = request.headers.get("userToken")

        admin_account, error_response = self.__checkAdminBanPermission(user_token)
        if error_response:
            return error_response

        try:
            banned_accounts = self.__adminUserBanService.getBannedAccounts()
            banned_list = []

            for account in banned_accounts:
                try:
                    decrypted_email = self.__decryptEmail(account)
                except Exception as e:
                    print(f"[ERROR] 이메일 복호화 실패: {str(e)}")
                    decrypted_email = account.email  # 복호화 실패 시 원래 이메일 유지

                banned_list.append({
                    "id": account.id,
                    "email": decrypted_email,
                    "banned_reason": account.banned_reason
                })
            return Response({"success": True, "banned_accounts": banned_list}, status=status.HTTP_200_OK)

        except Exception as e:
            return Response({"error": str(e), "success": False}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)