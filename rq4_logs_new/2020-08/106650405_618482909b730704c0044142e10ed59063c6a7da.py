#!/bin/env python
#-*-coding:utf-8-*-

import string
import sys 
reload(sys) 
sys.setdefaultencoding('utf8')
import ConfigParser
import logging
import logging.config
logging.config.fileConfig("etc/logger.ini")
logger = logging.getLogger("alert_main")

#import functions as func
from suds.client import Client
from xml.dom.minidom import parse
import xml.dom.minidom


def send_wx(content):
    try:
        sent_msg='''<?xml version="1.0" encoding="UTF-8"?> 
					<request>
                    <orgCode>3301060000000000000002</orgCode> 
					<sysCode>YWY</sysCode> 
					<text>%s</text>
                    </request>
        ''' %(content)
        
        client = Client('http://192.26.31.95:8080/sysManager/SysService?wsdl')
        result_xml=client.service.sendWxMessageText(sent_msg)
        
        #result_xml='''<?xml version="1.0" encoding="UTF-8"?><response><code>0</code><message>发送成功</message></response>'''
        
        DOMTree = xml.dom.minidom.parseString(result_xml)
        collection = DOMTree.documentElement
        codes = collection.getElementsByTagName("code")
        #print(codes[0].firstChild.data)
        res_code = codes[0].firstChild.data
        if res_code == '0':
            logger.info("Message: %s send wx success" %(content))
            return True
        else:
            logger.info("Message: %s send wx failed, return code: %s" %(content,res_code))
            return False
        	
        # Errorcode 定义
		# 200 //发送成功 3000 //参数内容解析失败 3001 //第三方应用信息认证失败 4000 //微信消息推送失败
    except Exception, e:
        logger.error("Message: %s send wx failed: %s" %(content, e.message))
        return False

if __name__ == '__main__':  
    send_wx('test')