#!/usr/bin/env python
# coding: utf-8

import time
import logging
from collections import namedtuple
import hashlib

try:
    from urlparse import urljoin
except ImportError:
    from urllib.parse import urljoin

from ifv.http_api import SimpleHTTPAPI

logger = logging.getLogger(__name__)


APIPathInfo = namedtuple("APIPathInfo", [
    "path", "method", "doc",
])
SignatureInfo = namedtuple("SignatureInfo", [
    "sid", "effect_millis", "nonce", "signature", "token",
])
RequestResult = namedtuple("RequestResult", [
    "errno", "errmsg", "data",
])


class IKeytoneAPIError(Exception):
    def __init__(self, message, response):
        super(IKeytoneAPIError, self).__init__(message)
        self.status_code = response.status_code
        self.response = response


class ResultParseError(IKeytoneAPIError):
    pass


class IKeytoneAPI(SimpleHTTPAPI):
    PARAMS_METHOD = ["GET", "DELETE"]
    PATH_META = {
        "domain": {
            "create": APIPathInfo(
                "domain/createDomain", "POST",
                u"创建新域",
            ),
            "destroy": APIPathInfo(
                "domain/destroyDomain", "DELETE",
                u"销毁无用域",
            ),
            "role": {
                "create": APIPathInfo(
                    "domain/createRole", "POST",
                    u"创建新角色",
                ),
                "destroy": APIPathInfo(
                    "domain/destroyRole", "DELETE",
                    u"销毁无用角色",
                ),
            },
            "user": {
                "create": APIPathInfo(
                    "domain/createUser", "POST",
                    u"创建用户",
                ),
                "enable": APIPathInfo(
                    "domain/enableUser", "PUT",
                    u"禁启用户",
                ),
                "destroy": APIPathInfo(
                    "domain/destroyUser", "DELETE",
                    u"销毁用户",
                ),
            },
            "project": {
                "create": APIPathInfo(
                    "domain/createProject", "POST",
                    u"创建项目",
                ),
                "enable": APIPathInfo(
                    "domain/enableProject", "PUT",
                    u"禁启项目",
                ),
                "destroy": APIPathInfo(
                    "domain/destroyProject", "DELETE",
                    u"销毁项目",
                ),
            },
            "user_role": {
                "add": APIPathInfo(
                    "domain/addUserRole", "POST",
                    u"添加用户角色",
                ),
                "get": APIPathInfo(
                    "domain/getUserRoles", "GET",
                    u"查询用户角色",
                ),
                "delete": APIPathInfo(
                    "domain/delUserRole", "DELETE",
                    u"删除用户角色",
                ),
            },
            "domain_user": {
                "get": APIPathInfo(
                    "domain/getDomainUser", "GET",
                    u"查询域用户",
                ),
            },
            "domain_project": {
                "get": APIPathInfo(
                    "domain/getDomainProject", "GET",
                    u"查询域项目",
                ),
            },
            "service": {
                "publish": APIPathInfo(
                    "domain/publishService", "PUT",
                    u"发布/更新服务",
                ),
                "lookup": APIPathInfo(
                    "domain/lookupService", "GET",
                    u"查询域服务API",
                )
            },
            "request": {
                "verify": APIPathInfo(
                    "domain/verifyRequest", "POST",
                    u"验证会话TOKEN",
                ),
            },
            "all_domain": {
                "get": APIPathInfo(
                    "domain/getAllDomain", "GET",
                    u"查询全部域",
                ),
            },
            "all_role": {
                "get": APIPathInfo(
                    "domain/getAllRole", "GET",
                    u"查询全部角色",
                ),
            },
            "session": {
                "create": APIPathInfo(
                    "domain/createSession", "POST",
                    u"创建会话TOKEN(域管理员代理需明确指定user)",
                ),
            },
            "passwd": {
                "modify": APIPathInfo(
                    "domain/modifyPasswd", "PUT",
                    u"修改用户密码(域管理员代理需明确指定user)",
                ),
            },
        },
        "system": {
            "allow_hosts": {
                "update": APIPathInfo(
                    "system/updateAllowHosts", "PUT",
                    u"刷新IP白名单",
                ),
            },
            "domain_cache": {
                "update": APIPathInfo(
                    "system/updateDomainCache", "PUT",
                    u"刷新全部域缓存",
                ),
            },
        },
    }

    def __init__(self, url, headers=None):
        super(IKeytoneAPI, self).__init__(url, headers)
        self._init_api_path(self, self.PATH_META)

    @classmethod
    def _init_api_path(cls, api_path, sub_info):
        for name, info in sub_info.items():
            sub_path = getattr(api_path, name)
            if isinstance(info, APIPathInfo):
                sub_path.__doc__ = info.doc
                sub_path._url_path = info.path
                sub_path._method = info.method
            else:
                cls._init_api_path(sub_path, info)

    def _get_url_and_method(self, api_path):
        return urljoin(self._url, api_path._url_path), api_path._method

    def _get_request(self, url, method, data=None, *args, **kwargs):
        if data:
            if method in self.PARAMS_METHOD:
                kwargs["params"] = data
            else:
                kwargs["json"] = data
        return super(IKeytoneAPI, self)._get_request(
            url, method, *args, **kwargs
        )

    def _get_result_from_response(self, response):
        try:
            result = response.json()
        except ValueError:
            raise ResultParseError("json loads failed", response)
        return RequestResult(
            errno=result.get("errno"),
            errmsg=result.get("errmsg"),
            data=result.get("data"),
        )

    @classmethod
    def get_signature_info(cls, sid, effect_millis, nonce=None):
        nonce = nonce or int(time.time() * 1000000)
        signature = hashlib.md5(
            "%s%x%x" % (sid, nonce, effect_millis)
        ).hexdigest()
        token = "%s-%x-%s" % (sid, nonce, signature)
        return SignatureInfo(
            sid=sid, effect_millis=effect_millis,
            nonce=nonce, signature=signature, token=token,
        )