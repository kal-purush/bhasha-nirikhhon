# -*- coding: utf-8 -*-
# code for console Encoding difference. Dont' mind on it 
import sys
import imp
imp.reload(sys)
try: sys.setdefaultencoding('UTF8')
except Exception as E: pass

import testValue

from popbill import MessageService, PopbillException

messageService =  MessageService(testValue.LinkID,testValue.SecretKey)
messageService.IsTest = testValue.IsTest

try:
    print("멀티메시지(MMS) 1건 전송")

    Sender = "07075103710" # 발신번호
    ReceiverNum = "010111222" # 수신번호
    ReceiverName = "수신자명"  # 수신자명
    Subject = "멀티 문자 제목" # 장문 메시지 제목
    Contents = "멀티메시지 단건전송 테스트" # 장문 메시지 내용, 길이가 2000Byte 초과시 길이가 조정되어 전송됨.
    reserveDT = "" # 예약전송시간, 형태 yyyyMMddHHmmss, 공백 처리시 즉시전송
    FilePath = 'test.jpeg' # 전송할 파일경로 

    receiptNum = messageService.sendMMS(testValue.testCorpNum,Sender,ReceiverNum,ReceiverName,Subject,Contents,FilePath,reserveDT)

    print("receiptNum : %s" % receiptNum)
    
except PopbillException as PE:
    print("Exception Occur : [%d] %s" % (PE.code , PE.message))