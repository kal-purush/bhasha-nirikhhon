from django.shortcuts import render
from rest_framework.viewsets import ViewSet
from rest_framework.response import Response
from django.http import JsonResponse
from account.entity.account import Account
from admin_user_info.service.admin_user_info_service_impl import AdminUserInfoServiceImpl
from account.service.account_service_impl import AccountServiceImpl
from redis_cache.service.redis_cache_service_impl import RedisCacheServiceImpl


class AdminUserInfoController(ViewSet):
    __accountService = AccountServiceImpl.getInstance()
    __adminUserInfoService = AdminUserInfoServiceImpl.getInstance()
    redisCacheService = RedisCacheServiceImpl.getInstance()



    # 관리자가 사용자의 모든 정보를 요청
    def getUserInfo(self, request, account_id):
        user_id = account_id
        target_account = self.__accountService.findAccountById(user_id)
        if not target_account:
            return JsonResponse({"error": "대상 사용자를 찾을 수 없습니다.", "success": False}, status=404)

        user_info = self.__adminUserInfoService.getUserInfo(user_id)
        if not user_info:
            return JsonResponse({"error": "사용자를 찾을 수 없습니다.", "success": False}, status=404)

        return JsonResponse({"success": True, "user_info": user_info}, status=200)



    # 관리자가 모든 사용자 들의 정보를 요청
    def getUserInfoList(self):
        pass

    # 사용자 이메일 복호화 하는 코드 추가

    # 관리자가 맞는지 확인하는 코드 추가

    # accountProfile 사용자 이름 복호화