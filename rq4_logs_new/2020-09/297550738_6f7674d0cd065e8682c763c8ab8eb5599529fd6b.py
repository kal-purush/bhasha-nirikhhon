"""
#coding=utf-8
@File    : jsonvalue.py
@Time    : 2020/9/12 14:04
@Author  : wanghao
@Email   : 947001731@qq.com
@Software: PyCharm
"""
import json

class jsonvalue(object):
    __result_list = []
    @classmethod
    def get(cls,user_dict, objkey):
        r_tuple_msg = cls._check_json_format(user_dict)
        if r_tuple_msg:
            for k, v in r_tuple_msg[1].items():
                if k == objkey:
                    cls.__result_list.append(v)
                elif type(v) is list:
                    for val in v:
                        if type(val) is dict:
                            for key, value in val.items():
                                if key == objkey:
                                    cls.__result_list.append(value)
                else:
                    t = cls._check_json_format(v)
                    if t:
                        if t[0]:
                            if type(t[1]) is dict:
                                cls.get(t[1], objkey)
                            elif type(t[1]) is list:
                                for val in t[1]:
                                    if type(val) is dict:
                                        for key, value in val.items():
                                            if key == objkey:
                                                cls.__result_list.append(value)
            if len(cls.__result_list) == 1:
                return ''.join(cls.__result_list)
            elif len(cls.__result_list) < 1:
                raise KeyError("Not found {}".format(objkey))
            else:
                return cls.__result_list
        else:
            raise ValueError("No JSON object could be decoded")

    @staticmethod
    def _check_json_format(raw_msg):
        if isinstance(raw_msg, str):
            try:
                raw_msg = json.loads(raw_msg)
            except ValueError:
                return False
            else:
                return True, raw_msg
        elif isinstance(raw_msg, dict):
            return True, raw_msg
        else:
            return False