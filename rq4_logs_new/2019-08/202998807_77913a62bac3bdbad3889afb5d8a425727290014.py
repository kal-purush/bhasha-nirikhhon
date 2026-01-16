from app.ApiResource import api
from flask_restful import Resource
from app.models import *


# @api.resource("/hello/")
# class Hello(Resource):
#     def get(self):
#         return {"hello":"world"}

result = {
    "version": "v1.0",
    "code": "", #请求状态码，可以自定义，也可以使用http 200成功 300跳转 400页面错误 500服务器错误
    "data":[

    ]
}
@api.resource("/user/")
class Hello(Resource):
    def get(self):
        values = User.query.all()
        result["code"] = 200
        for value in values:
            result["data"].append(
                        {
                            value.username:{
                                "username": value.username,
                                "password": value.password,
                                "identity": value.identity,
                                "is_active": True
                            }
                        }
                    )
            result["count"] = len(values)
        return result

#做了判断的，刷新不能进行添加
# @api.resource("/user/")
# class Hello(Resource):
#     def get(self):
#         if len(result["data"])==0:
#             values = User.query.all()
#             result["code"] = 200
#             for value in values:
#                     result["data"].append(
#                         {
#                             value.username:{
#                                 "username": value.username,
#                                 "password": value.password,
#                                 "identity": value.identity,
#                                 "is_active": True
#                             }
#                         }
#                     )
#             result["count"] = len(values)
#         return result
