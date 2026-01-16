# -*- coding: utf-8 -*-
import logging
from django.http import HttpResponse
import json
import time
import sys
sys.path.append('...')
from ctb.models import user
from ctb.models import carInfo
from ctb.models import taskInfo
from ctb.models import getTask
from ctb.models import doTask
from ctb.models import checkRecord
from ctb.models import outStream
from ctb.models import incomeStream
from .. import Comm
from .. import Jurisdiction
from ..task import TaskInfo


# 获取车辆的信息
def getCarInfo(request):
    callBackDict = {}
    # 验证用户的openID
    userObj = Jurisdiction.jurisdictGETOpenId(request, callBackDict)
    if userObj == None:
        return callBackDict
    # 车辆的ID信息
    getcarId = Comm.tryTranslate(request, "carId")
    if Comm.tryTranslateNull("carId", getcarId, callBackDict) == False:
        return callBackDict
    try:
        onecarInfo = carInfo.objects.get(id = getcarId)
        imgsJosn = json.loads(onecarInfo.adImgs)
        Comm.callBackSuccess(callBackDict, 1, {"id": onecarInfo.id, "carNum": onecarInfo.carNum, "carModel": onecarInfo.carModel,
                     "remark": onecarInfo.remark, "adImgs": imgsJosn})
    except BaseException as e:
        logger = logging.getLogger("django")
        logger.info(str(e))
        Comm.callBackFail(callBackDict,-1,"系统异常")
    return callBackDict




# 获取领取任务的信息
def getGetTaskInfo(request):
    callBackDict = {}
    # 验证用户的openID
    userObj = Jurisdiction.jurisdictGETOpenId(request, callBackDict)
    if userObj == None:
        return callBackDict
    # 领取任务的ID
    getTaskId = Comm.tryTranslate(request, "getTaskId")
    if Comm.tryTranslateNull("getTaskId", getTaskId, callBackDict) == False:
        return callBackDict
    try:
        getTaskObj = getTask.objects.get(id=getTaskId)
        # 获取车辆和任务的信息
        dataDict = {}
        getBaseTaskAndCarInfo(getTaskObj.taskId,getTaskObj.carId,dataDict)
        dataDict["id"] = getTaskObj.id
        dataDict["userId"] = getTaskObj.userId
        dataDict["openId"] = getTaskObj.openId
        dataDict["createTime"] = getTaskObj.createTime
        dataDict["status"] = getTaskObj.status
        dataDict["startdoTaskTime"] = getTaskObj.startdoTaskTime
        Comm.callBackSuccess(callBackDict,1,dataDict)
    except BaseException as e:
        logger = logging.getLogger("django")
        logger.info(str(e))
        Comm.callBackFail(callBackDict,-1,"系统异常")
    return callBackDict




# 获取做任务的信息
def getDoTaskInfo(request):
    callBackDict = {}
    # 验证用户的openID
    userObj = Jurisdiction.jurisdictGETOpenId(request, callBackDict)
    if userObj == None:
        return callBackDict
    # 做任务的ID
    getdoTaskId = Comm.tryTranslate(request, "doTaskId")
    if Comm.tryTranslateNull("doTaskId", getdoTaskId, callBackDict) == False:
        return callBackDict
    try:
        doTaskObj = doTask.objects.get(id=getdoTaskId)
        dataDict = {}
        # 获取领取的任务信息
        getTaskObj = getTask.objects.get(id=doTaskObj.getTaskId)
        # 获取车辆和任务的信息
        getBaseTaskAndCarInfo(getTaskObj.taskId, getTaskObj.carId, dataDict)
        dataDict["id"] = doTaskObj.id
        dataDict["userId"] = doTaskObj.userId
        dataDict["openId"] = doTaskObj.openId
        dataDict["createTime"] = doTaskObj.createTime
        dataDict["status"] = doTaskObj.status
        dataDict["getTaskId"] = doTaskObj.getTaskId
        dataDict["latitude"] = doTaskObj.latitude
        dataDict["longitude"] = doTaskObj.longitude
        dataDict["status"] = doTaskObj.status
        dataDict["adImgs"] = json.loads(doTaskObj.adImgs)
        Comm.callBackSuccess(callBackDict, 1, dataDict)
    except BaseException as e:
        logger = logging.getLogger("django")
        logger.info(str(e))
        Comm.callBackFail(callBackDict, -1, "系统异常")
    return callBackDict



# 获取车辆和任务的信息
def getBaseTaskAndCarInfo(taskId,carId,callBackDict):
    # 获取任务信息
    try:
        oneTaskInfo = taskInfo.objects.get(id=taskId)
        callBackDict["taskInfo"] = TaskInfo.makeDictaskInfo(oneTaskInfo)
    except BaseException as e:
        logger = logging.getLogger("django")
        logger.info(str(e))
    # 获取车辆信息
    try:
        onecarInfo = carInfo.objects.get(id=carId)
        imgsJosn = json.loads(onecarInfo.adImgs)
        callBackDict["carInfo"] = {"id": onecarInfo.id, "carNum": onecarInfo.carNum, "carModel": onecarInfo.carModel,
                                   "remark": onecarInfo.remark, "adImgs": imgsJosn}
    except BaseException as e:
       logger = logging.getLogger("django")
       logger.info(str(e))