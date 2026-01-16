#!/usr/bin/env python
#coding=utf-8

import subprocess
import requests
import datetime
from hashlib import sha1
import hmac
import base64
import ConfigParser
import time
import hashlib
import os
import inspect
from BaseFunc import getfileinfo
from xml.etree import ElementTree
import multiprocessing
import sys
reload(sys)
sys.setdefaultencoding("utf-8")
sys.path.append('..')
import unittest
import urllib
from BaseFunc import generate_token,compare_xml_tag,code_message_info,compare_xml_200,compare_xml_200_specialstring,get_md5_value

#获取当前执行函数的名称
def get_current_function_name():
    return inspect.stack()[1][3]

#用例内容
class myUI_AbortMultipartuploadid(unittest.TestCase):
    def setUp(self):
        self.cf = ConfigParser.ConfigParser()
        curDir = os.path.abspath('.') + os.path.sep
        self.filePath = curDir + os.path.sep
        self.fileinfo = self.filePath+'Uploadslice.config'
        with open(self.fileinfo) as file:
            self.cf.readfp(file)  # 读取文件信息
            self.bucket = self.cf.get('bucket', 'bucket')  #
            self.invalidbucket = self.cf.get('bucket', 'invalidbucket')#无效的空间名称
            self.undobucket = self.cf.get('bucket', 'undobucket')#无法操作的空间名称
            self.nomatchbucket = self.cf.get('bucket','nomatchbucket')#不属于当前用户的空间error_bucket
            self.errorbucket = self.cf.get('bucket', 'error_bucket')#无法操作的空间名称。不存在
            #########################################文件名称#############
            self.minetype = self.cf.get('filename', 'minetype')#w无后缀的文件
            self.filename = self.cf.get('filename', 'filename')#文件名称,值有一个分片的文件，大小为12m
            self.filename_4m = self.cf.get('filename','filename_4m') #只有一个分片，且分片大小为4m
            self.filename_first_less4m = self.cf.get('filename','filename_first_less4m') #第一个分片的大小小于4m
            self.filename_last_less4m = self.cf.get('filename','filename_last_less4m') #最后一个分片的大小小于4m
            self.xmlfile = self.cf.get('filename','xmlfile')#xml文件名称
            self.errorxmlfile = self.cf.get('filename','errorxmlfile')
            ###########################################文件地址@###############
            self.filepath_4m = self.cf.get('filename','slicepath')#分片文件地址,只有一个分片文件,文件大小为4m
            self.filepath_first_less4m = self.cf.get('filename','filepath_first_less4m')#第一个分片文件大小小于4m
            self.filepath_last_less4m = self.cf.get('filename','filepath_last_less4m')#第一个分片文件大小小于4m
            self.slicepath = self.cf.get('filename','slicepath')#分片文件地址,只有一个分片文件
            self.moreslicepath = self.cf.get('filename','moreslicepath')#多个分片文件地址
            ##################################################################
            self.resource_path = self.cf.get('filename','resource_path') #原始文件，后面用于对比下载文件的md5值是否与其一致
            self.download_path = self.cf.get('download','savepath') +self.filename  #下载后的文件地址，后面用于比对是否和原文件一致
            self.s3_url = self.cf.get('urlinfo','s3_url')#s3 rul地址
            self.S3_ACCESS_KEY_ID = self.cf.get('key','ak')
            self.S3_SECRET_ACCESS_KEY = self.cf.get('key','sk')
            self.errorak = self.cf.get('key','errorak')
            self.errorsk = self.cf.get('key','errorsk')
            self.uploadid = ''
            ########################分片文件信息##############################
            self.slice_list = getfileinfo(self.slicepath)
            ######################列举参数####################################
            self.maxparts=3
            self.partnumbermarker=1
            ########获取http所需的GMT格式时间#################################
            GMT_FORMAT = '%a, %d %b %Y %H:%M:%S GMT'
            self.date = datetime.datetime.utcnow().strftime(GMT_FORMAT)
            GMT_FORMAT_before = '%a, %d %b %Y %H:%M:%S GMT'
            self.date_before = (datetime.datetime.utcnow()-datetime.timedelta(minutes=16)).strftime(GMT_FORMAT_before)
            #获取自定义的xamzdate时间值##################################
            time.sleep(0.5)
            self.xamzdate = datetime.datetime.utcnow().strftime(GMT_FORMAT)
            ############初始化队列列荣#######################
            manager = multiprocessing.Manager()
            self.queue = manager.Queue()
            ###############设置返回success格式###################
            self.success= '\033[32m########################################\n                 SUCCESS\n########################################\033[0m'
            self.fail= '\033[31m########################################\n                 FAIL\n########################################\033[0m'
    '''
    三个初始化函数
    @1正常的请求初始化，获取uploadid ，并写入到配置文件中
    @2正确的分片上传请求，上传分片数据
    @3遍历要上传的分片，循环上传
    '''
    def InitialMultipartUpload_ok(self,objectname):
        '初始化，获取分片和合成使用到的uplaodid'
        print '>>>>当前执行函数名称:',"%s.%s"%(self.__class__.__name__,get_current_function_name())
        resource='/'+self.bucket+'/'+objectname+'?uploads'
        print '>>>>resource信息：',resource
        contentType=''
        contentMD5=''
        method='POST'
        date=self.date
        #token的算是 HMAC_SHA1算法，然后再次进行base64加密
        #HMAC运算利用哈希算法，以一个密钥和一个消息为输入，生成一个消息摘要作为输出
        token = generate_token(method,contentMD5,contentType,date,resource,self.S3_ACCESS_KEY_ID,self.S3_SECRET_ACCESS_KEY)
        print '>>>>鉴权后的token信息:\n',token
        headers = {'Authorization':token,'Date':self.date}
        print '>>>>请求头部信息headers:\n',headers
        s3url = self.s3_url+resource
        print '>>>>请求的s3url:\n',self.s3_url
        rq = requests.post(url=s3url,headers=headers,timeout=30)
        status =  str(rq.status_code)
        print '响应头部信息：',rq.headers
        if status == '200':
            return_data = rq.text
            print rq.headers
            print '>>>>响应状态码：',status
            print '>>>>返回结果：\n', rq.text
            #获取当前执行得到upploadid，并写入配置文件
            cf = ConfigParser.ConfigParser()
            cfgfile =cf.read("MultipartUpload.config")
            rootxml = ElementTree.fromstring(return_data)
            for  child in rootxml.findall('{http://wcs.chinanetcenter.com/document}UploadId'):
                self.uploadid = child.text
            print '初始化的uploadid：',self.uploadid
        else:
            print '响应的错误码:',status
            print rq.text
            print self.fail
            return 'FAIL'

    def MultipartUpload_ok(self,objectname,slicepath,slicename,partnum):
        '分片上传，传入文件地址，文件名称objectname，上传的partnum'
        print '>>>>当前执行函数名称:',"%s.%s"%(self.__class__.__name__,get_current_function_name())
        #进程共享数据
        partnum_etag_dict = {}
        #分段文件名称
        objectname = objectname #文件的名称
        print '>>>>本次上传的文件名称：',objectname
        file_path = slicepath+slicename
        print '>>>>分片文件地址信息：',slicepath
        #加载分段文件
        with open(file_path,'rb') as f:
            # md5值获取，如果文件太大，分行获取，防止内存加载过多
            file_data = f.read()
            self.md5 = get_md5_value(file_data)
            #resource='/{0}/{1}'.format(self.bucket,objectname)
            resource = '/{0}/{1}?partNumber={2}&uploadId={3}'.format(self.bucket,objectname,partnum,self.uploadid)
            print '>>>>resource信息:',resource
            contentType=''
            contentMD5=''
            method='PUT'
            date = self.date
            #token的算是 HMAC_SHA1算法，然后再次进行base64加密
            #HMAC运算利用哈希算法，以一个密钥和一个消息为输入，生成一个消息摘要作为输出
            token = generate_token(method,contentMD5,contentType,date,resource,self.S3_ACCESS_KEY_ID,self.S3_SECRET_ACCESS_KEY)
            #获取最后的token
            print '>>>>token信息：',token
            print '>>>>uploadid信息',self.uploadid
            #头部信息###############
            headers = {'Authorization': token,  'Date': self.date}
            print '>>>>headers:', headers
            ######组合url地址######################################################
            s3url = '{0}{1}'.format(self.s3_url,resource)
            print '>>>>s3url:', s3url
            #####put 上传分段文件数据##############################################
            rq = requests.put(s3url,data=file_data ,headers=headers,timeout=300)
            status = str(rq.status_code)
            print '>>>>返回状态码：',status
            #分片上传响应etag值在头部中
            return_header =  rq.headers
            print '>>>>返回头部结果：', return_header
            #对返回状态码做判断，并返回比对结果
            if status == '200':
                etag = return_header['ETag']
                partnum_etag_dict['PartNumber']=str(partnum)
                partnum_etag_dict['ETag']=etag.strip('"')
                print '>>>>头部获取到的[etag]信息：',etag
                self.queue.put(str(partnum_etag_dict))
                print self.success
                return 'SUCCESS'
            else:
                self.errortext = rq.text
                print '分片上传失败:', self.errortext
                print self.fail
                return 'FAIL'

       #多线程执行分片上传任务

    def run_upload(self,objectname,slicepath):
        print '>>>>当前执行函数名称:',"%s.%s"%(self.__class__.__name__,get_current_function_name())
        '遍历所有要上传你的分片，并上传分片文件'
        self.InitialMultipartUpload_ok(objectname)
        xml_etag = []
        threads=[]
        #对返回的分片文件名称进行排序，从小到大
        slice_list = getfileinfo(slicepath)
        slice_list_sorted = sorted(slice_list[0].keys())
        print slice_list_sorted
        for index,key in enumerate(slice_list_sorted):
            result = self.MultipartUpload_ok(objectname,slicepath,key,index+1)
            if result !='SUCCESS':
                break
        if result == 'SUCCESS':
            while self.queue.qsize():
                xml_etag.append(eval(self.queue.get()))
            print '>>>>待写入的partnum和etag信息：',xml_etag
            print self.success
        else:
            print '分片上传失败，无法合成文件'

    '''
    @正常的请求:删除uplaodid。然后再对这个uploadid上传分片，要无法成功
    '''
    def test_AbortMultipartUploadid(self):
        print '>>>>当前执行函数名称:',"%s.%s"%(self.__class__.__name__,get_current_function_name())
        self.run_upload(self.filename,self.slicepath)
        uploadid = 'VIVKlbJlIz17TkWsrPT07Vcvd27GU7X3Ya3qt2Kqna47rpHd25o2aqvAsBUPKB8u1XEkPzKosiBmMBi8724S2A--'
        #打印当前执行函数的名称
        print '>>>>当前执行函数名称:',"%s.%s"%(self.__class__.__name__,get_current_function_name())
        resource= '/{0}/{1}?uploadId={2}'.format(self.bucket,self.filename,self.uploadid )
        #'/'+self.bucket+'/'+self.filename+'?uploads'
        print '>>>>resource信息：',resource
        contentType=''
        contentMD5=''
        method='DELETE'
        date=self.date
        #token的算是 HMAC_SHA1算法，然后再次进行base64加密
        #HMAC运算利用哈希算法，以一个密钥和一个消息为输入，生成一个消息摘要作为输出
        token = generate_token(method,contentMD5,contentType,date,resource,self.S3_ACCESS_KEY_ID,self.S3_SECRET_ACCESS_KEY)
        print '>>>>鉴权后的token信息:\n',token
        headers = {'Authorization':token,'Date':self.date}
        print '>>>>请求头部信息headers:\n',headers
        s3url = self.s3_url+resource
        print '>>>>请求的s3url:\n',s3url
        rq = requests.delete(url=s3url,headers=headers)
        status =  str(rq.status_code)
        print u'返回状态码 ',status
        print '响应头部信息：',rq.headers
        return_data = rq.text #接口返回的内容
        print return_data
        if status == '200':
            print '清除分片上传任务成功！'
            #结果返回200时候，再次进行分片上传，这时候接口返回的应该是uploadid 不存在
            print '开始验证分片上传的uploadid是否被清除......'
            upload_reslut = self.MultipartUpload_ok(self.filename,self.slicepath,'aws00','1')
            if upload_reslut == 'FAIL':
                result = compare_xml_tag(self.errortext,code_message_info.status_404,code_message_info.code_404_NoSuchUpload,code_message_info.message_404_NoSuchUpload)
                if result =='SUCCESS':
                    print '分片uploadid验证已被删除'
            else:
                print '验证失败，分片仍可以上传！'
                print self.fail
                result = 'FAIL'
        else:
            print self.fail
            result = 'FAIL'
        self.assertEquals(result, "SUCCESS")

    '''
    @正常的请求:列举分片数据，uploadid不存在
    '''
    def test_AbortMultipartUploadid_uploadid_isnotexist(self):
        print '>>>>当前执行函数名称:',"%s.%s"%(self.__class__.__name__,get_current_function_name())
        #self.run_upload(self.filename,self.slicepath)
        #self.run_upload(self.filename,self.slicepath)
        uploadid = 'VIVKlbJlIz17TkWsrPT07Vcvd27GU7X3Ya3qt2Kqna47rpHd25o2aqvAsBUPKB8u1XEkPzKosiBmMBi8724S2A--'
        #打印当前执行函数的名称
        print '>>>>当前执行函数名称:',"%s.%s"%(self.__class__.__name__,get_current_function_name())
        resource= '/{0}/{1}?uploadId={2}'.format(self.bucket,self.filename,uploadid )
        #'/'+self.bucket+'/'+self.filename+'?uploads'
        print '>>>>resource信息：',resource
        contentType=''
        contentMD5=''
        method='DELETE'
        date=self.date
        #token的算是 HMAC_SHA1算法，然后再次进行base64加密
        #HMAC运算利用哈希算法，以一个密钥和一个消息为输入，生成一个消息摘要作为输出
        token = generate_token(method,contentMD5,contentType,date,resource,self.S3_ACCESS_KEY_ID,self.S3_SECRET_ACCESS_KEY)
        print '>>>>鉴权后的token信息:\n',token
        headers = {'Authorization':token,'Date':self.date}
        print '>>>>请求头部信息headers:\n',headers
        s3url = self.s3_url+resource
        print '>>>>请求的s3url:\n',s3url
        rq = requests.delete(url=s3url,headers=headers)
        status =  str(rq.status_code)
        print u'返回状态码 ',status
        print '响应头部信息：',rq.headers
        return_data = rq.text #接口返回的内容
        print return_data
        result = compare_xml_tag(return_data,status,
                                 code_message_info.status_404,
                                 code_message_info.code_404_NoSuchUpload,
                                 code_message_info.message_404_list_NoSuchUpload)
        self.assertEquals(result, "SUCCESS")

    '''
    @正常的请求:异常请求数据: 鉴权中的uploadid拼写错误，url中的uploadid 也拼写同样的错误

    '''
    def test_AbortMultipartUploadid_uploadid_iserror(self):
        print '>>>>当前执行函数名称:',"%s.%s"%(self.__class__.__name__,get_current_function_name())
        #self.run_upload(self.filename,self.slicepath)
        #self.run_upload(self.filename,self.slicepath)
        uploadid = 'VIVKlbJlIz17TkWsrPT07Vcvd27GU7X3Ya3qt2Kqna47rpHd25o2aqvAsBUPKB8u1XEkPzKosiBmMBi8724S2A--'
        #打印当前执行函数的名称
        print '>>>>当前执行函数名称:',"%s.%s"%(self.__class__.__name__,get_current_function_name())
        resource= '/{0}/{1}?uploadDDId={2}'.format(self.bucket,self.filename,uploadid )
        #'/'+self.bucket+'/'+self.filename+'?uploads'
        print '>>>>resource信息：',resource
        contentType=''
        contentMD5=''
        method='DELETE'
        date=self.date
        #token的算是 HMAC_SHA1算法，然后再次进行base64加密
        #HMAC运算利用哈希算法，以一个密钥和一个消息为输入，生成一个消息摘要作为输出
        token = generate_token(method,contentMD5,contentType,date,resource,self.S3_ACCESS_KEY_ID,self.S3_SECRET_ACCESS_KEY)
        print '>>>>鉴权后的token信息:\n',token
        headers = {'Authorization':token,'Date':self.date}
        print '>>>>请求头部信息headers:\n',headers
        s3url = self.s3_url+resource
        print '>>>>请求的s3url:\n',s3url
        rq = requests.delete(url=s3url,headers=headers)
        status =  str(rq.status_code)
        print u'返回状态码 ',status
        print '响应头部信息：',rq.headers
        return_data = rq.text #接口返回的内容
        print return_data
        result = compare_xml_tag(return_data,status,
                                 code_message_info.status_403,
                                 code_message_info.code_403_SignatureDoesNotMatch,
                                 code_message_info.message_403_SignatureDoesNotMatch)
        self.assertEquals(result, "SUCCESS")


    '''
    @正常的请求:空间不存在
    '''
    def test_AbortMultipartUploadid_bucket_isnoexist(self):
        print '>>>>当前执行函数名称:',"%s.%s"%(self.__class__.__name__,get_current_function_name())
        self.run_upload(self.filename,self.slicepath)
        #self.run_upload(self.filename,self.slicepath)
        #uploadid = 'VIVKlbJlIz17TkWsrPT07Vcvd27GU7X3Ya3qt2Kqna47rpHd25o2aqvAsBUPKB8u1XEkPzKosiBmMBi8724S2A--'
        #打印当前执行函数的名称
        print '>>>>当前执行函数名称:',"%s.%s"%(self.__class__.__name__,get_current_function_name())
        resource= '/{0}/{1}?uploadDDId={2}'.format(self.errorbucket,self.filename,self.uploadid )
        #'/'+self.bucket+'/'+self.filename+'?uploads'
        print '>>>>resource信息：',resource
        contentType=''
        contentMD5=''
        method='DELETE'
        date=self.date
        #token的算是 HMAC_SHA1算法，然后再次进行base64加密
        #HMAC运算利用哈希算法，以一个密钥和一个消息为输入，生成一个消息摘要作为输出
        token = generate_token(method,contentMD5,contentType,date,resource,self.S3_ACCESS_KEY_ID,self.S3_SECRET_ACCESS_KEY)
        print '>>>>鉴权后的token信息:\n',token
        headers = {'Authorization':token,'Date':self.date}
        print '>>>>请求头部信息headers:\n',headers
        s3url = self.s3_url+resource
        print '>>>>请求的s3url:\n',s3url
        rq = requests.delete(url=s3url,headers=headers)
        status =  str(rq.status_code)
        print u'返回状态码 ',status
        print '响应头部信息：',rq.headers
        return_data = rq.text #接口返回的内容
        print return_data
        result = compare_xml_tag(return_data,status,
                                 code_message_info.status_404,
                                 code_message_info.code_404_NoSuchBucket,
                                 code_message_info.message_404_NoSuchBucket)
        self.assertEquals(result, "SUCCESS")


    '''
    @正常的请求:用户请求的空间名无效。（空间被删除，无效）
    '''
    def test_AbortMultipartUploadid_bucket_InvalidBucketState(self):
        print '>>>>当前执行函数名称:',"%s.%s"%(self.__class__.__name__,get_current_function_name())
        #self.run_upload(self.filename,self.slicepath)
        #self.run_upload(self.filename,self.slicepath)
        uploadid = 'VIVKlbJlIz17TkWsrPT07Vcvd27GU7X3Ya3qt2Kqna47rpHd25o2aqvAsBUPKB8u1XEkPzKosiBmMBi8724S2A--'
        #打印当前执行函数的名称
        print '>>>>当前执行函数名称:',"%s.%s"%(self.__class__.__name__,get_current_function_name())
        resource= '/{0}/{1}?uploadId={2}'.format(self.invalidbucket,self.filename,self.uploadid )
        #'/'+self.bucket+'/'+self.filename+'?uploads'
        print '>>>>resource信息：',resource
        contentType=''
        contentMD5=''
        method='DELETE'
        date=self.date
        #token的算是 HMAC_SHA1算法，然后再次进行base64加密
        #HMAC运算利用哈希算法，以一个密钥和一个消息为输入，生成一个消息摘要作为输出
        token = generate_token(method,contentMD5,contentType,date,resource,self.S3_ACCESS_KEY_ID,self.S3_SECRET_ACCESS_KEY)
        print '>>>>鉴权后的token信息:\n',token
        headers = {'Authorization':token,'Date':self.date}
        print '>>>>请求头部信息headers:\n',headers
        s3url = self.s3_url+resource
        print '>>>>请求的s3url:\n',s3url
        rq = requests.delete(url=s3url,headers=headers)
        status =  str(rq.status_code)
        print u'返回状态码 ',status
        print '响应头部信息：',rq.headers
        return_data = rq.text #接口返回的内容
        print return_data
        result = compare_xml_tag(return_data,status,
                                 code_message_info.status_409,
                                 code_message_info.code_409_nvalidBucketState,
                                 code_message_info.message_409_nvalidBucketState)
        self.assertEquals(result, "SUCCESS")

    '''
    @正常的请求:用户请求的空间不存在
    '''
    def test_AbortMultipartUploadid_bucket_error(self):
        print '>>>>当前执行函数名称:',"%s.%s"%(self.__class__.__name__,get_current_function_name())
        #self.run_upload(self.filename,self.slicepath)
        #self.run_upload(self.filename,self.slicepath)
        uploadid = 'VIVKlbJlIz17TkWsrPT07Vcvd27GU7X3Ya3qt2Kqna47rpHd25o2aqvAsBUPKB8u1XEkPzKosiBmMBi8724S2A--'
        #打印当前执行函数的名称
        print '>>>>当前执行函数名称:',"%s.%s"%(self.__class__.__name__,get_current_function_name())
        resource= '/{0}/{1}?uploadId={2}'.format(self.errorbucket,self.filename,self.uploadid )
        #'/'+self.bucket+'/'+self.filename+'?uploads'
        print '>>>>resource信息：',resource
        contentType=''
        contentMD5=''
        method='DELETE'
        date=self.date
        #token的算是 HMAC_SHA1算法，然后再次进行base64加密
        #HMAC运算利用哈希算法，以一个密钥和一个消息为输入，生成一个消息摘要作为输出
        token = generate_token(method,contentMD5,contentType,date,resource,self.S3_ACCESS_KEY_ID,self.S3_SECRET_ACCESS_KEY)
        print '>>>>鉴权后的token信息:\n',token
        headers = {'Authorization':token,'Date':self.date}
        print '>>>>请求头部信息headers:\n',headers
        s3url = self.s3_url+resource
        print '>>>>请求的s3url:\n',s3url
        rq = requests.delete(url=s3url,headers=headers)
        status =  str(rq.status_code)
        print u'返回状态码 ',status
        print '响应头部信息：',rq.headers
        return_data = rq.text #接口返回的内容
        print return_data
        result = compare_xml_tag(return_data,status,
                                 code_message_info.status_404,
                                 code_message_info.code_404_NoSuchBucket,
                                 code_message_info.message_404_NoSuchBucket)
        self.assertEquals(result, "SUCCESS")

    '''
    @正常的请求:请求中的Key过长，最大字节不超过500字节
    '''
    def test_AbortMultipartUploadid_keytoolong(self):
        print '>>>>当前执行函数名称:',"%s.%s"%(self.__class__.__name__,get_current_function_name())
        #self.run_upload(self.filename,self.slicepath)
        #self.run_upload(self.filename,self.slicepath)
        uploadid = 'VIVKlbJlIz17TkWsrPT07Vcvd27GU7X3Ya3qt2Kqna47rpHd25o2aqvAsBUPKB8u1XEkPzKosiBmMBi8724S2A--'
        filename =  'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' \
                       +'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'\
                       +'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'\
                       +'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'\
                       +'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'\
                       +'.mp4'
        #打印当前执行函数的名称
        print '>>>>当前执行函数名称:',"%s.%s"%(self.__class__.__name__,get_current_function_name())
        resource= '/{0}/{1}?uploadId={2}'.format(self.bucket,filename,self.uploadid )
        #'/'+self.bucket+'/'+self.filename+'?uploads'
        print '>>>>resource信息：',resource
        contentType=''
        contentMD5=''
        method='DELETE'
        date=self.date
        #token的算是 HMAC_SHA1算法，然后再次进行base64加密
        #HMAC运算利用哈希算法，以一个密钥和一个消息为输入，生成一个消息摘要作为输出
        token = generate_token(method,contentMD5,contentType,date,resource,self.S3_ACCESS_KEY_ID,self.S3_SECRET_ACCESS_KEY)
        print '>>>>鉴权后的token信息:\n',token
        headers = {'Authorization':token,'Date':self.date}
        print '>>>>请求头部信息headers:\n',headers
        s3url = self.s3_url+resource
        print '>>>>请求的s3url:\n',s3url
        rq = requests.delete(url=s3url,headers=headers)
        status =  str(rq.status_code)
        print u'返回状态码 ',status
        print '响应头部信息：',rq.headers
        return_data = rq.text #接口返回的内容
        print return_data
        result = compare_xml_tag(return_data,status,
                                 code_message_info.status_400,
                                 code_message_info.code_400_KeyTooLong,
                                 code_message_info.message_400_KeyTooLong)
        self.assertEquals(result, "SUCCESS")


    '''
    @正常的请求:：子资源的空间不属于当前aksk用户
    '''
    def test_AbortMultipartUploadid_aksk_nomatch(self):
        print '>>>>当前执行函数名称:',"%s.%s"%(self.__class__.__name__,get_current_function_name())
        #self.run_upload(self.filename,self.slicepath)
        #self.run_upload(self.filename,self.slicepath)
        uploadid = 'VIVKlbJlIz17TkWsrPT07Vcvd27GU7X3Ya3qt2Kqna47rpHd25o2aqvAsBUPKB8u1XEkPzKosiBmMBi8724S2A--'
        #打印当前执行函数的名称
        print '>>>>当前执行函数名称:',"%s.%s"%(self.__class__.__name__,get_current_function_name())
        resource= '/{0}/{1}?uploadId={2}'.format(self.nomatchbucket,self.filename,self.uploadid )
        #'/'+self.bucket+'/'+self.filename+'?uploads'
        print '>>>>resource信息：',resource
        contentType=''
        contentMD5=''
        method='DELETE'
        date=self.date
        #token的算是 HMAC_SHA1算法，然后再次进行base64加密
        #HMAC运算利用哈希算法，以一个密钥和一个消息为输入，生成一个消息摘要作为输出
        token = generate_token(method,contentMD5,contentType,date,resource,self.S3_ACCESS_KEY_ID,self.S3_SECRET_ACCESS_KEY)
        print '>>>>鉴权后的token信息:\n',token
        headers = {'Authorization':token,'Date':self.date}
        print '>>>>请求头部信息headers:\n',headers
        s3url = self.s3_url+resource
        print '>>>>请求的s3url:\n',s3url
        rq = requests.delete(url=s3url,headers=headers)
        status =  str(rq.status_code)
        print u'返回状态码 ',status
        print '响应头部信息：',rq.headers
        return_data = rq.text #接口返回的内容
        print return_data
        result = compare_xml_tag(return_data,status,
                                 code_message_info.status_403,
                                 code_message_info.code_403_AccessDenied,
                                 code_message_info.message_403_AccessDenied_3)
        self.assertEquals(result, "SUCCESS")


    '''
    @正常的请求:：用户提供的AccessKeyId不存在
    '''
    def test_AbortMultipartUploadid_aksk_iserror(self):
        print '>>>>当前执行函数名称:',"%s.%s"%(self.__class__.__name__,get_current_function_name())
        #self.run_upload(self.filename,self.slicepath)
        #self.run_upload(self.filename,self.slicepath)
        uploadid = 'VIVKlbJlIz17TkWsrPT07Vcvd27GU7X3Ya3qt2Kqna47rpHd25o2aqvAsBUPKB8u1XEkPzKosiBmMBi8724S2A--'
        #打印当前执行函数的名称
        print '>>>>当前执行函数名称:',"%s.%s"%(self.__class__.__name__,get_current_function_name())
        resource= '/{0}/{1}?uploadId={2}'.format(self.bucket,self.filename,self.uploadid )
        #'/'+self.bucket+'/'+self.filename+'?uploads'
        print '>>>>resource信息：',resource
        contentType=''
        contentMD5=''
        method='DELETE'
        date=self.date
        #token的算是 HMAC_SHA1算法，然后再次进行base64加密
        #HMAC运算利用哈希算法，以一个密钥和一个消息为输入，生成一个消息摘要作为输出
        token = generate_token(method,contentMD5,contentType,date,resource,self.errorak,self.errorsk)
        print '>>>>鉴权后的token信息:\n',token
        headers = {'Authorization':token,'Date':self.date}
        print '>>>>请求头部信息headers:\n',headers
        s3url = self.s3_url+resource
        print '>>>>请求的s3url:\n',s3url
        rq = requests.delete(url=s3url,headers=headers)
        status =  str(rq.status_code)
        print u'返回状态码 ',status
        print '响应头部信息：',rq.headers
        return_data = rq.text #接口返回的内容
        print return_data
        result = compare_xml_tag(return_data,status,
                                 code_message_info.status_403,
                                 code_message_info.code_403_InvalidAccessKeyId,
                                 code_message_info.message_403_InvalidAccessKeyId)
        self.assertEquals(result, "SUCCESS")

    '''
    @正常的请求:：用户请求的时间与服务器的时间相差太大，最大十五分钟
    '''
    def test_AbortMultipartUploadid_token_outime(self):
        print '>>>>当前执行函数名称:',"%s.%s"%(self.__class__.__name__,get_current_function_name())
        #self.run_upload(self.filename,self.slicepath)
        #self.run_upload(self.filename,self.slicepath)
        uploadid = 'VIVKlbJlIz17TkWsrPT07Vcvd27GU7X3Ya3qt2Kqna47rpHd25o2aqvAsBUPKB8u1XEkPzKosiBmMBi8724S2A--'
        #打印当前执行函数的名称
        print '>>>>当前执行函数名称:',"%s.%s"%(self.__class__.__name__,get_current_function_name())
        resource= '/{0}/{1}?uploadId={2}'.format(self.bucket,self.filename,self.uploadid )
        #'/'+self.bucket+'/'+self.filename+'?uploads'
        print '>>>>resource信息：',resource
        contentType=''
        contentMD5=''
        method='DELETE'
        date=self.date_before
        #token的算是 HMAC_SHA1算法，然后再次进行base64加密
        #HMAC运算利用哈希算法，以一个密钥和一个消息为输入，生成一个消息摘要作为输出
        token = generate_token(method,contentMD5,contentType,date,resource,self.S3_ACCESS_KEY_ID,self.S3_SECRET_ACCESS_KEY)
        print '>>>>鉴权后的token信息:\n',token
        headers = {'Authorization':token,'Date':self.date_before}
        print '>>>>请求头部信息headers:\n',headers
        s3url = self.s3_url+resource
        print '>>>>请求的s3url:\n',s3url
        rq = requests.delete(url=s3url,headers=headers)
        status =  str(rq.status_code)
        print u'返回状态码 ',status
        print '响应头部信息：',rq.headers
        return_data = rq.text #接口返回的内容
        print return_data
        result = compare_xml_tag(return_data,status,
                                 code_message_info.status_400,
                                 code_message_info.code_400_ExpiredToken,
                                 code_message_info.message_400_ExpiredToken)
        self.assertEquals(result, "SUCCESS")


    '''
    @正常的请求:：文件名称带特殊字符，进行urlencode 编码（与WCS不支持的字符保持一致），比如反斜杠和`
    '''
    def test_AbortMultipartUploadid_Specialcharacters(self):
        print '>>>>当前执行函数名称:',"%s.%s"%(self.__class__.__name__,get_current_function_name())
        #self.run_upload(self.filename,self.slicepath)
        filename=urllib.quote('!@#$%^&*.mp4')
        self.run_upload(filename,self.slicepath)
        uploadid = 'VIVKlbJlIz17TkWsrPT07Vcvd27GU7X3Ya3qt2Kqna47rpHd25o2aqvAsBUPKB8u1XEkPzKosiBmMBi8724S2A--'
        #打印当前执行函数的名称
        print '>>>>当前执行函数名称:',"%s.%s"%(self.__class__.__name__,get_current_function_name())
        resource= '/{0}/{1}?uploadId={2}'.format(self.bucket,filename,self.uploadid )
        #'/'+self.bucket+'/'+self.filename+'?uploads'
        print '>>>>resource信息：',resource
        contentType=''
        contentMD5=''
        method='DELETE'
        date=self.date
        #token的算是 HMAC_SHA1算法，然后再次进行base64加密
        #HMAC运算利用哈希算法，以一个密钥和一个消息为输入，生成一个消息摘要作为输出
        token = generate_token(method,contentMD5,contentType,date,resource,self.S3_ACCESS_KEY_ID,self.S3_SECRET_ACCESS_KEY)
        print '>>>>鉴权后的token信息:\n',token
        headers = {'Authorization':token,'Date':self.date}
        print '>>>>请求头部信息headers:\n',headers
        s3url = self.s3_url+resource
        print '>>>>请求的s3url:\n',s3url
        rq = requests.delete(url=s3url,headers=headers)
        status =  str(rq.status_code)
        print u'返回状态码 ',status
        print '响应头部信息：',rq.headers
        return_data = rq.text #接口返回的内容
        print return_data
        if status == '200':
            print '清除分片上传任务成功！'
            #结果返回200时候，再次进行分片上传，这时候接口返回的应该是uploadid 不存在
            print '开始验证分片上传的uploadid是否被清除......'
            upload_reslut = self.MultipartUpload_ok(self.filename,self.slicepath,'aws00','1')
            if upload_reslut == 'FAIL':
                result = compare_xml_tag(self.errortext,code_message_info.status_404,code_message_info.code_404_NoSuchUpload,code_message_info.message_404_NoSuchUpload)
                if result =='SUCCESS':
                    print '分片uploadid验证已被删除'
            else:
                print '验证失败，分片仍可以上传！'
                print self.fail
                result = 'FAIL'
        else:
            print self.fail
            result = 'FAIL'
        self.assertEquals(result, "SUCCESS")


    '''
    @正常的请求:：文件对象名称带中文名，需要进行urlencode 编码
    '''
    def test_AbortMultipartUploadid_filename_ischinese(self):
        print '>>>>当前执行函数名称:',"%s.%s"%(self.__class__.__name__,get_current_function_name())
        #self.run_upload(self.filename,self.slicepath)
        filename=urllib.quote('中文.mp4')
        self.run_upload(filename,self.slicepath)
        uploadid = 'VIVKlbJlIz17TkWsrPT07Vcvd27GU7X3Ya3qt2Kqna47rpHd25o2aqvAsBUPKB8u1XEkPzKosiBmMBi8724S2A--'
        #打印当前执行函数的名称
        print '>>>>当前执行函数名称:',"%s.%s"%(self.__class__.__name__,get_current_function_name())
        resource= '/{0}/{1}?uploadId={2}'.format(self.bucket,filename,self.uploadid )
        #'/'+self.bucket+'/'+self.filename+'?uploads'
        print '>>>>resource信息：',resource
        contentType=''
        contentMD5=''
        method='DELETE'
        date=self.date
        #token的算是 HMAC_SHA1算法，然后再次进行base64加密
        #HMAC运算利用哈希算法，以一个密钥和一个消息为输入，生成一个消息摘要作为输出
        token = generate_token(method,contentMD5,contentType,date,resource,self.S3_ACCESS_KEY_ID,self.S3_SECRET_ACCESS_KEY)
        print '>>>>鉴权后的token信息:\n',token
        headers = {'Authorization':token,'Date':self.date}
        print '>>>>请求头部信息headers:\n',headers
        s3url = self.s3_url+resource
        print '>>>>请求的s3url:\n',s3url
        rq = requests.delete(url=s3url,headers=headers)
        status =  str(rq.status_code)
        print u'返回状态码 ',status
        print '响应头部信息：',rq.headers
        return_data = rq.text #接口返回的内容
        print return_data
        if status == '200':
            print '清除分片上传任务成功！'
            #结果返回200时候，再次进行分片上传，这时候接口返回的应该是uploadid 不存在
            print '开始验证分片上传的uploadid是否被清除......'
            upload_reslut = self.MultipartUpload_ok(self.filename,self.slicepath,'aws00','1')
            if upload_reslut == 'FAIL':
                result = compare_xml_tag(self.errortext,
                                         code_message_info.status_404,
                                         code_message_info.status_404,
                                         code_message_info.code_404_NoSuchUpload,
                                         code_message_info.message_404_NoSuchUpload)
                if result =='SUCCESS':
                    print '分片uploadid验证已被删除'
            else:
                print '验证失败，分片仍可以上传！'
                print self.fail
                result = 'FAIL'
        else:
            print self.fail
            result = 'FAIL'
        self.assertEquals(result, "SUCCESS")


    '''
    @正常的请求:：header请求头部：x-amz-server-side-encryption 不支持
    '''
    def test_AbortMultipartUploadid_header_unsupoort_xamzserversideencryptioncustomerkeyMD5(self):
        print '>>>>当前执行函数名称:',"%s.%s"%(self.__class__.__name__,get_current_function_name())
        #self.run_upload(self.filename,self.slicepath)
        #self.run_upload(self.filename,self.slicepath)
        uploadid = 'VIVKlbJlIz17TkWsrPT07Vcvd27GU7X3Ya3qt2Kqna47rpHd25o2aqvAsBUPKB8u1XEkPzKosiBmMBi8724S2A--'
        #打印当前执行函数的名称
        print '>>>>当前执行函数名称:',"%s.%s"%(self.__class__.__name__,get_current_function_name())
        resource= '/{0}/{1}?uploadId={2}'.format(self.bucket,self.filename,self.uploadid )
        #'/'+self.bucket+'/'+self.filename+'?uploads'
        print '>>>>resource信息：',resource
        contentType=''
        contentMD5=''
        method='DELETE'
        date=self.date
        #token的算是 HMAC_SHA1算法，然后再次进行base64加密
        #HMAC运算利用哈希算法，以一个密钥和一个消息为输入，生成一个消息摘要作为输出
        token = generate_token(method,contentMD5,contentType,date,resource,self.S3_ACCESS_KEY_ID,self.S3_SECRET_ACCESS_KEY)
        print '>>>>鉴权后的token信息:\n',token
        headers = {'Authorization':token,'Date':self.date,'x-amz-server-side-encryption-customer-key-MD5':'DDDDDDDD'}
        print '>>>>请求头部信息headers:\n',headers
        s3url = self.s3_url+resource
        print '>>>>请求的s3url:\n',s3url
        rq = requests.delete(url=s3url,headers=headers)
        status =  str(rq.status_code)
        print u'返回状态码 ',status
        print '响应头部信息：',rq.headers
        return_data = rq.text #接口返回的内容
        print return_data
        result = compare_xml_tag(return_data,status,
                                 code_message_info.status_501,
                                 code_message_info.code_501,
                                 code_message_info.message_501)
        self.assertEquals(result, "SUCCESS")



    '''
    @正常的请求:：header请求消息头不带：Authorization：token
    '''
    def test_AbortMultipartUploadid_header_misstoken(self):
        print '>>>>当前执行函数名称:',"%s.%s"%(self.__class__.__name__,get_current_function_name())
        #self.run_upload(self.filename,self.slicepath)
        #self.run_upload(self.filename,self.slicepath)
        uploadid = 'VIVKlbJlIz17TkWsrPT07Vcvd27GU7X3Ya3qt2Kqna47rpHd25o2aqvAsBUPKB8u1XEkPzKosiBmMBi8724S2A--'
        #打印当前执行函数的名称
        print '>>>>当前执行函数名称:',"%s.%s"%(self.__class__.__name__,get_current_function_name())
        resource= '/{0}/{1}?uploadId={2}'.format(self.bucket,self.filename,self.uploadid )
        #'/'+self.bucket+'/'+self.filename+'?uploads'
        print '>>>>resource信息：',resource
        contentType=''
        contentMD5=''
        method='DELETE'
        date=self.date
        #token的算是 HMAC_SHA1算法，然后再次进行base64加密
        #HMAC运算利用哈希算法，以一个密钥和一个消息为输入，生成一个消息摘要作为输出
        token = generate_token(method,contentMD5,contentType,date,resource,self.S3_ACCESS_KEY_ID,self.S3_SECRET_ACCESS_KEY)
        print '>>>>鉴权后的token信息:\n',token
        headers = {'Authorization':'','Date':self.date}
        print '>>>>请求头部信息headers:\n',headers
        s3url = self.s3_url+resource
        print '>>>>请求的s3url:\n',s3url
        rq = requests.delete(url=s3url,headers=headers)
        status =  str(rq.status_code)
        print u'返回状态码 ',status
        print '响应头部信息：',rq.headers
        return_data = rq.text #接口返回的内容
        print return_data
        result = compare_xml_tag(return_data,status,
                                 code_message_info.status_403,
                                 code_message_info.code_403_AccessDenied,
                                 code_message_info.message_403_AccessDenied_2)
        self.assertEquals(result, "SUCCESS")


    '''
    @正常的请求:：header中的content-type为中文
    '''
    def test_AbortMultipartUploadid_header_contenttype_ischinese(self):
        print '>>>>当前执行函数名称:',"%s.%s"%(self.__class__.__name__,get_current_function_name())
        self.run_upload(self.filename,self.slicepath)
        #self.run_upload(self.filename,self.slicepath)
        uploadid = 'VIVKlbJlIz17TkWsrPT07Vcvd27GU7X3Ya3qt2Kqna47rpHd25o2aqvAsBUPKB8u1XEkPzKosiBmMBi8724S2A--'
        #打印当前执行函数的名称
        print '>>>>当前执行函数名称:',"%s.%s"%(self.__class__.__name__,get_current_function_name())
        resource= '/{0}/{1}?uploadId={2}'.format(self.bucket,self.filename,self.uploadid )
        #'/'+self.bucket+'/'+self.filename+'?uploads'
        print '>>>>resource信息：',resource
        stringaa = '#$@%@中文'
        contentType=stringaa
        contentMD5=''
        method='DELETE'
        date=self.date
        #token的算是 HMAC_SHA1算法，然后再次进行base64加密
        #HMAC运算利用哈希算法，以一个密钥和一个消息为输入，生成一个消息摘要作为输出
        token = generate_token(method,contentMD5,contentType,date,resource,self.S3_ACCESS_KEY_ID,self.S3_SECRET_ACCESS_KEY)
        print '>>>>鉴权后的token信息:\n',token
        headers = {'Authorization':token,'Date':self.date,'Content-Type':stringaa}
        print '>>>>请求头部信息headers:\n',headers
        s3url = self.s3_url+resource
        print '>>>>请求的s3url:\n',s3url
        rq = requests.delete(url=s3url,headers=headers)
        status =  str(rq.status_code)
        print u'返回状态码 ',status
        print '响应头部信息：',rq.headers
        return_data = rq.text #接口返回的内容
        print return_data
        result = compare_xml_tag(return_data,status,
                                 code_message_info.status_403,
                                 code_message_info.code_403_SignatureDoesNotMatch,
                                 code_message_info.message_403_SignatureDoesNotMatch)
        self.assertEquals(result, "SUCCESS")

    '''
    @正常的请求:：header请求消息头：header带content-type，StringToSign中不带content-type
    '''
    def test_AbortMultipartUploadid_header_contenttype_StringToSign_not(self):
        print '>>>>当前执行函数名称:',"%s.%s"%(self.__class__.__name__,get_current_function_name())
        #self.run_upload(self.filename,self.slicepath)
        #self.run_upload(self.filename,self.slicepath)
        uploadid = 'VIVKlbJlIz17TkWsrPT07Vcvd27GU7X3Ya3qt2Kqna47rpHd25o2aqvAsBUPKB8u1XEkPzKosiBmMBi8724S2A--'
        #打印当前执行函数的名称
        print '>>>>当前执行函数名称:',"%s.%s"%(self.__class__.__name__,get_current_function_name())
        resource= '/{0}/{1}?uploadId={2}'.format(self.bucket,self.filename,self.uploadid )
        #'/'+self.bucket+'/'+self.filename+'?uploads'
        print '>>>>resource信息：',resource
        contentType=''
        contentMD5=''
        method='DELETE'
        date=self.date
        #token的算是 HMAC_SHA1算法，然后再次进行base64加密
        #HMAC运算利用哈希算法，以一个密钥和一个消息为输入，生成一个消息摘要作为输出
        token = generate_token(method,contentMD5,contentType,date,resource,self.S3_ACCESS_KEY_ID,self.S3_SECRET_ACCESS_KEY)
        print '>>>>鉴权后的token信息:\n',token
        headers = {'Authorization':token,'Date':self.date,'Content-Type':'中文'}
        print '>>>>请求头部信息headers:\n',headers
        s3url = self.s3_url+resource
        print '>>>>请求的s3url:\n',s3url
        rq = requests.delete(url=s3url,headers=headers)
        status =  str(rq.status_code)
        print u'返回状态码 ',status
        print '响应头部信息：',rq.headers
        return_data = rq.text #接口返回的内容
        print return_data
        result = compare_xml_tag(return_data,status,
                                 code_message_info.status_403,
                                 code_message_info.code_403_SignatureDoesNotMatch,
                                 code_message_info.message_403_SignatureDoesNotMatch)
        self.assertEquals(result, "SUCCESS")


    '''
    @正常的请求:：header请求消息头：header不带content-type，StringToSign中带content-type
    '''
    def test_AbortMultipartUploadid_StringToSign_contenttype_header_not(self):
        print '>>>>当前执行函数名称:',"%s.%s"%(self.__class__.__name__,get_current_function_name())
        #self.run_upload(self.filename,self.slicepath)
        #self.run_upload(self.filename,self.slicepath)
        uploadid = 'VIVKlbJlIz17TkWsrPT07Vcvd27GU7X3Ya3qt2Kqna47rpHd25o2aqvAsBUPKB8u1XEkPzKosiBmMBi8724S2A--'
        #打印当前执行函数的名称
        print '>>>>当前执行函数名称:',"%s.%s"%(self.__class__.__name__,get_current_function_name())
        resource= '/{0}/{1}?uploadId={2}'.format(self.bucket,self.filename,self.uploadid )
        #'/'+self.bucket+'/'+self.filename+'?uploads'
        print '>>>>resource信息：',resource
        contentType='text/plain'
        contentMD5=''
        method='DELETE'
        date=self.date
        #token的算是 HMAC_SHA1算法，然后再次进行base64加密
        #HMAC运算利用哈希算法，以一个密钥和一个消息为输入，生成一个消息摘要作为输出
        token = generate_token(method,contentMD5,contentType,date,resource,self.S3_ACCESS_KEY_ID,self.S3_SECRET_ACCESS_KEY)
        print '>>>>鉴权后的token信息:\n',token
        headers = {'Authorization':token,'Date':self.date}
        print '>>>>请求头部信息headers:\n',headers
        s3url = self.s3_url+resource
        print '>>>>请求的s3url:\n',s3url
        rq = requests.delete(url=s3url,headers=headers)
        status =  str(rq.status_code)
        print u'返回状态码 ',status
        print '响应头部信息：',rq.headers
        return_data = rq.text #接口返回的内容
        print return_data
        result = compare_xml_tag(return_data,status,
                                 code_message_info.status_403,
                                 code_message_info.code_403_SignatureDoesNotMatch,
                                 code_message_info.message_403_SignatureDoesNotMatch)
        self.assertEquals(result, "SUCCESS")


    '''
    @正常的请求:：header请求消息头：header带content-type，StringToSign中带content-type
    '''
    def test_AbortMultipartUploadid_StringToSign_and_header_addcontenttype(self):
        print '>>>>当前执行函数名称:',"%s.%s"%(self.__class__.__name__,get_current_function_name())
        self.run_upload(self.filename,self.slicepath)
        #self.run_upload(self.filename,self.slicepath)
        uploadid = 'VIVKlbJlIz17TkWsrPT07Vcvd27GU7X3Ya3qt2Kqna47rpHd25o2aqvAsBUPKB8u1XEkPzKosiBmMBi8724S2A--'
        #打印当前执行函数的名称
        print '>>>>当前执行函数名称:',"%s.%s"%(self.__class__.__name__,get_current_function_name())
        resource= '/{0}/{1}?uploadId={2}'.format(self.bucket,self.filename,self.uploadid )
        #'/'+self.bucket+'/'+self.filename+'?uploads'
        print '>>>>resource信息：',resource
        contentType='text/plain'
        contentMD5=''
        method='DELETE'
        date=self.date
        #token的算是 HMAC_SHA1算法，然后再次进行base64加密
        #HMAC运算利用哈希算法，以一个密钥和一个消息为输入，生成一个消息摘要作为输出
        token = generate_token(method,contentMD5,contentType,date,resource,self.S3_ACCESS_KEY_ID,self.S3_SECRET_ACCESS_KEY)
        print '>>>>鉴权后的token信息:\n',token
        headers = {'Authorization':token,'Date':self.date,'Content-Type':'text/plain'}
        print '>>>>请求头部信息headers:\n',headers
        s3url = self.s3_url+resource
        print '>>>>请求的s3url:\n',s3url
        rq = requests.delete(url=s3url,headers=headers)
        status =  str(rq.status_code)
        print u'返回状态码 ',status
        print '响应头部信息：',rq.headers
        return_data = rq.text #接口返回的内容
        print return_data
        if status == '200':
            print '清除分片上传任务成功！'
            #结果返回200时候，再次进行分片上传，这时候接口返回的应该是uploadid 不存在
            print '开始验证分片上传的uploadid是否被清除......'
            upload_reslut = self.MultipartUpload_ok(self.filename,self.slicepath,'aws00','1')
            if upload_reslut == 'FAIL':
                result = compare_xml_tag(self.errortext,
                                         code_message_info.status_404,
                                         code_message_info.status_404,
                                         code_message_info.code_404_NoSuchUpload,
                                         code_message_info.message_404_NoSuchUpload)
                if result =='SUCCESS':
                    print '分片uploadid验证已被删除'
            else:
                print '验证失败，分片仍可以上传！'
                print self.fail
                result = 'FAIL'
        else:
            print self.fail
            result = 'FAIL'
        self.assertEquals(result, "SUCCESS")


    '''
    @正常的请求:：header头部中带md5，StringToSign鉴权中不带有md5
    '''
    def test_AbortMultipartUploadid_header_addmd5_StringToSign_not(self):
        print '>>>>当前执行函数名称:',"%s.%s"%(self.__class__.__name__,get_current_function_name())
        self.run_upload(self.filename,self.slicepath)
        #self.run_upload(self.filename,self.slicepath)
        uploadid = 'VIVKlbJlIz17TkWsrPT07Vcvd27GU7X3Ya3qt2Kqna47rpHd25o2aqvAsBUPKB8u1XEkPzKosiBmMBi8724S2A--'
        #打印当前执行函数的名称
        print '>>>>当前执行函数名称:',"%s.%s"%(self.__class__.__name__,get_current_function_name())
        resource= '/{0}/{1}?uploadId={2}'.format(self.bucket,self.filename,self.uploadid )
        #'/'+self.bucket+'/'+self.filename+'?uploads'
        print '>>>>resource信息：',resource
        contentType=''
        contentMD5=''
        method='DELETE'
        date=self.date
        #token的算是 HMAC_SHA1算法，然后再次进行base64加密
        #HMAC运算利用哈希算法，以一个密钥和一个消息为输入，生成一个消息摘要作为输出
        token = generate_token(method,contentMD5,contentType,date,resource,self.S3_ACCESS_KEY_ID,self.S3_SECRET_ACCESS_KEY)
        print '>>>>鉴权后的token信息:\n',token
        headers = {'Authorization':token,'Date':self.date,'Content-MD5':'DDDDDDDDDDD'}
        print '>>>>请求头部信息headers:\n',headers
        s3url = self.s3_url+resource
        print '>>>>请求的s3url:\n',s3url
        rq = requests.delete(url=s3url,headers=headers)
        status =  str(rq.status_code)
        print u'返回状态码 ',status
        print '响应头部信息：',rq.headers
        return_data = rq.text #接口返回的内容
        print return_data
        result = compare_xml_tag(return_data,
                                 status,
                                 code_message_info.status_403,
                                 code_message_info.code_403_SignatureDoesNotMatch,
                                 code_message_info.message_403_SignatureDoesNotMatch)
        self.assertEquals(result, "SUCCESS")


    '''
    @正常的请求:：header 头部带date，StringToSign鉴权不带date
    '''
    def test_AbortMultipartUploadid_header_adddate_StringToSign_not(self):
        print '>>>>当前执行函数名称:',"%s.%s"%(self.__class__.__name__,get_current_function_name())
        #self.run_upload(self.filename,self.slicepath)
        #self.run_upload(self.filename,self.slicepath)
        uploadid = 'VIVKlbJlIz17TkWsrPT07Vcvd27GU7X3Ya3qt2Kqna47rpHd25o2aqvAsBUPKB8u1XEkPzKosiBmMBi8724S2A--'
        #打印当前执行函数的名称
        print '>>>>当前执行函数名称:',"%s.%s"%(self.__class__.__name__,get_current_function_name())
        resource= '/{0}/{1}?uploadId={2}'.format(self.bucket,self.filename,self.uploadid )
        #'/'+self.bucket+'/'+self.filename+'?uploads'
        print '>>>>resource信息：',resource
        contentType=''
        contentMD5=''
        method='DELETE'
        date=''
        #token的算是 HMAC_SHA1算法，然后再次进行base64加密
        #HMAC运算利用哈希算法，以一个密钥和一个消息为输入，生成一个消息摘要作为输出
        token = generate_token(method,contentMD5,contentType,date,resource,self.S3_ACCESS_KEY_ID,self.S3_SECRET_ACCESS_KEY)
        print '>>>>鉴权后的token信息:\n',token
        headers = {'Authorization':token,'Date':self.date}
        print '>>>>请求头部信息headers:\n',headers
        s3url = self.s3_url+resource
        print '>>>>请求的s3url:\n',s3url
        rq = requests.delete(url=s3url,headers=headers)
        status =  str(rq.status_code)
        print u'返回状态码 ',status
        print '响应头部信息：',rq.headers
        return_data = rq.text #接口返回的内容
        print return_data
        result = compare_xml_tag(return_data,
                                 status,
                                 code_message_info.status_403,
                                 code_message_info.code_403_SignatureDoesNotMatch,
                                 code_message_info.message_403_SignatureDoesNotMatch)
        self.assertEquals(result, "SUCCESS")

    '''
    @正常的请求:：header 头部不带date，StringToSign鉴权带date
    '''
    def test_AbortMultipartUploadid_StringToSign_adddate_header_not(self):
        print '>>>>当前执行函数名称:',"%s.%s"%(self.__class__.__name__,get_current_function_name())
        #self.run_upload(self.filename,self.slicepath)
        #self.run_upload(self.filename,self.slicepath)
        uploadid = 'VIVKlbJlIz17TkWsrPT07Vcvd27GU7X3Ya3qt2Kqna47rpHd25o2aqvAsBUPKB8u1XEkPzKosiBmMBi8724S2A--'
        #打印当前执行函数的名称
        print '>>>>当前执行函数名称:',"%s.%s"%(self.__class__.__name__,get_current_function_name())
        resource= '/{0}/{1}?uploadId={2}'.format(self.bucket,self.filename,self.uploadid )
        #'/'+self.bucket+'/'+self.filename+'?uploads'
        print '>>>>resource信息：',resource
        contentType=''
        contentMD5=''
        method='DELETE'
        date=self.date
        #token的算是 HMAC_SHA1算法，然后再次进行base64加密
        #HMAC运算利用哈希算法，以一个密钥和一个消息为输入，生成一个消息摘要作为输出
        token = generate_token(method,contentMD5,contentType,date,resource,self.S3_ACCESS_KEY_ID,self.S3_SECRET_ACCESS_KEY)
        print '>>>>鉴权后的token信息:\n',token
        headers = {'Authorization':token}
        print '>>>>请求头部信息headers:\n',headers
        s3url = self.s3_url+resource
        print '>>>>请求的s3url:\n',s3url
        rq = requests.delete(url=s3url,headers=headers)
        status =  str(rq.status_code)
        print u'返回状态码 ',status
        print '响应头部信息：',rq.headers
        return_data = rq.text #接口返回的内容
        print return_data
        result = compare_xml_tag(return_data,
                                 status,
                                 code_message_info.status_403,
                                 code_message_info.code_403_AccessDenied,
                                 code_message_info.message_403_AccessDenied)
        self.assertEquals(result, "SUCCESS")



    '''
    @正常的请求:：header 头部不带date，StringToSign鉴权不带date
    '''
    def test_AbortMultipartUploadid_StringToSign_header_notdate(self):
        print '>>>>当前执行函数名称:',"%s.%s"%(self.__class__.__name__,get_current_function_name())
        #self.run_upload(self.filename,self.slicepath)
        #self.run_upload(self.filename,self.slicepath)
        uploadid = 'VIVKlbJlIz17TkWsrPT07Vcvd27GU7X3Ya3qt2Kqna47rpHd25o2aqvAsBUPKB8u1XEkPzKosiBmMBi8724S2A--'
        #打印当前执行函数的名称
        print '>>>>当前执行函数名称:',"%s.%s"%(self.__class__.__name__,get_current_function_name())
        resource= '/{0}/{1}?uploadId={2}'.format(self.bucket,self.filename,self.uploadid )
        #'/'+self.bucket+'/'+self.filename+'?uploads'
        print '>>>>resource信息：',resource
        contentType=''
        contentMD5=''
        method='DELETE'
        date=''
        #token的算是 HMAC_SHA1算法，然后再次进行base64加密
        #HMAC运算利用哈希算法，以一个密钥和一个消息为输入，生成一个消息摘要作为输出
        token = generate_token(method,contentMD5,contentType,date,resource,self.S3_ACCESS_KEY_ID,self.S3_SECRET_ACCESS_KEY)
        print '>>>>鉴权后的token信息:\n',token
        headers = {'Authorization':token}
        print '>>>>请求头部信息headers:\n',headers
        s3url = self.s3_url+resource
        print '>>>>请求的s3url:\n',s3url
        rq = requests.delete(url=s3url,headers=headers)
        status =  str(rq.status_code)
        print u'返回状态码 ',status
        print '响应头部信息：',rq.headers
        return_data = rq.text #接口返回的内容
        print return_data
        result = compare_xml_tag(return_data,
                                 status,
                                 code_message_info.status_403,
                                 code_message_info.code_403_AccessDenied,
                                 code_message_info.message_403_AccessDenied)
        self.assertEquals(result, "SUCCESS")


    '''
    @正常的请求:：header请求消息头带：x-amz-date：当请求中出现Authorization时，则请求中必须明确头部Date或者x-amz-date。如果请求中头部Date和x-amz-date同时定义，则以头部x-amz-date为主。
    '''
    def test_AbortMultipartUploadid_StringToSign_and_head_add_xamzdate(self):
        print '>>>>当前执行函数名称:',"%s.%s"%(self.__class__.__name__,get_current_function_name())
        self.run_upload(self.filename,self.slicepath)
        #self.run_upload(self.filename,self.slicepath)
        uploadid = 'VIVKlbJlIz17TkWsrPT07Vcvd27GU7X3Ya3qt2Kqna47rpHd25o2aqvAsBUPKB8u1XEkPzKosiBmMBi8724S2A--'
        #打印当前执行函数的名称
        print '>>>>当前执行函数名称:',"%s.%s"%(self.__class__.__name__,get_current_function_name())
        resource= '/{0}/{1}?uploadId={2}'.format(self.bucket,self.filename,self.uploadid )
        #'/'+self.bucket+'/'+self.filename+'?uploads'
        print '>>>>resource信息：',resource
        contentType=''
        contentMD5=''
        method='DELETE'
        date=''
        xamzdate = self.date
        #token的算是 HMAC_SHA1算法，然后再次进行base64加密
        #HMAC运算利用哈希算法，以一个密钥和一个消息为输入，生成一个消息摘要作为输出
        token = generate_token(method,contentMD5,contentType,date,resource,self.S3_ACCESS_KEY_ID,self.S3_SECRET_ACCESS_KEY,xamzdate)
        print '>>>>鉴权后的token信息:\n',token
        headers = {'Authorization':token,'x-amz-date': self.date}
        print '>>>>请求头部信息headers:\n',headers
        s3url = self.s3_url+resource
        print '>>>>请求的s3url:\n',s3url
        rq = requests.delete(url=s3url,headers=headers)
        status =  str(rq.status_code)
        print u'返回状态码 ',status
        print '响应头部信息：',rq.headers
        return_data = rq.text #接口返回的内容
        print return_data
        if status == '200':
            print '清除分片上传任务成功！'
            #结果返回200时候，再次进行分片上传，这时候接口返回的应该是uploadid 不存在
            print '开始验证分片上传的uploadid是否被清除......'
            upload_reslut = self.MultipartUpload_ok(self.filename,self.slicepath,'aws00','1')
            if upload_reslut == 'FAIL':
                result = compare_xml_tag(self.errortext,
                                         code_message_info.status_404,
                                         code_message_info.status_404,
                                         code_message_info.code_404_NoSuchUpload,
                                         code_message_info.message_404_NoSuchUpload)
                if result =='SUCCESS':
                    print '分片uploadid验证已被删除'
            else:
                print '验证失败，分片仍可以上传！'
                print self.fail
                result = 'FAIL'
        else:
            print self.fail
            result = 'FAIL'
        self.assertEquals(result, "SUCCESS")


    '''
    @正常的请求:：header 头部带x-amz-date，StringToSign带date且不带x-amz-date
    '''
    def test_AbortMultipartUploadid_head_add_xamzdate_StringToSign_not(self):
        print '>>>>当前执行函数名称:',"%s.%s"%(self.__class__.__name__,get_current_function_name())
        #self.run_upload(self.filename,self.slicepath)
        #self.run_upload(self.filename,self.slicepath)
        uploadid = 'VIVKlbJlIz17TkWsrPT07Vcvd27GU7X3Ya3qt2Kqna47rpHd25o2aqvAsBUPKB8u1XEkPzKosiBmMBi8724S2A--'
        #打印当前执行函数的名称
        print '>>>>当前执行函数名称:',"%s.%s"%(self.__class__.__name__,get_current_function_name())
        resource= '/{0}/{1}?uploadId={2}'.format(self.bucket,self.filename,self.uploadid )
        #'/'+self.bucket+'/'+self.filename+'?uploads'
        print '>>>>resource信息：',resource
        contentType=''
        contentMD5=''
        method='DELETE'
        date=''
        xamzdate = self.date
        #token的算是 HMAC_SHA1算法，然后再次进行base64加密
        #HMAC运算利用哈希算法，以一个密钥和一个消息为输入，生成一个消息摘要作为输出
        token = generate_token(method,contentMD5,contentType,date,resource,self.S3_ACCESS_KEY_ID,self.S3_SECRET_ACCESS_KEY)
        print '>>>>鉴权后的token信息:\n',token
        headers = {'Authorization':token,'x-amz-date': self.date}
        print '>>>>请求头部信息headers:\n',headers
        s3url = self.s3_url+resource
        print '>>>>请求的s3url:\n',s3url
        rq = requests.delete(url=s3url,headers=headers)
        status =  str(rq.status_code)
        print u'返回状态码 ',status
        print '响应头部信息：',rq.headers
        return_data = rq.text #接口返回的内容
        print return_data
        result = compare_xml_tag(return_data,
                                 status,
                                 code_message_info.status_403,
                                 code_message_info.code_403_SignatureDoesNotMatch,
                                 code_message_info.message_403_SignatureDoesNotMatch)
        self.assertEquals(result, "SUCCESS")


    '''
    @正常的请求:：header 头部不带x-amz-date，不带date，StringToSign带x-amz-date
    '''
    def test_AbortMultipartUploadid_StringToSign_add_xamzdate_header_not(self):
        print '>>>>当前执行函数名称:',"%s.%s"%(self.__class__.__name__,get_current_function_name())
        #self.run_upload(self.filename,self.slicepath)
        #self.run_upload(self.filename,self.slicepath)
        uploadid = 'VIVKlbJlIz17TkWsrPT07Vcvd27GU7X3Ya3qt2Kqna47rpHd25o2aqvAsBUPKB8u1XEkPzKosiBmMBi8724S2A--'
        #打印当前执行函数的名称
        print '>>>>当前执行函数名称:',"%s.%s"%(self.__class__.__name__,get_current_function_name())
        resource= '/{0}/{1}?uploadId={2}'.format(self.bucket,self.filename,self.uploadid )
        #'/'+self.bucket+'/'+self.filename+'?uploads'
        print '>>>>resource信息：',resource
        contentType=''
        contentMD5=''
        method='DELETE'
        date=''
        xamzdate = self.date
        #token的算是 HMAC_SHA1算法，然后再次进行base64加密
        #HMAC运算利用哈希算法，以一个密钥和一个消息为输入，生成一个消息摘要作为输出
        token = generate_token(method,contentMD5,contentType,date,resource,self.S3_ACCESS_KEY_ID,self.S3_SECRET_ACCESS_KEY,xamzdate)
        print '>>>>鉴权后的token信息:\n',token
        headers = {'Authorization':token}
        print '>>>>请求头部信息headers:\n',headers
        s3url = self.s3_url+resource
        print '>>>>请求的s3url:\n',s3url
        rq = requests.delete(url=s3url,headers=headers)
        status =  str(rq.status_code)
        print u'返回状态码 ',status
        print '响应头部信息：',rq.headers
        return_data = rq.text #接口返回的内容
        print return_data
        result = compare_xml_tag(return_data,
                                 status,
                                 code_message_info.status_403,
                                 code_message_info.code_403_AccessDenied,
                                 code_message_info.message_403_AccessDenied)
        self.assertEquals(result, "SUCCESS")


    '''
    @正常的请求:：header 头部中的x-amz-date写错，写成x-az-date
    '''
    def test_AbortMultipartUploadid_StringToSign_add_xamzdate_header_error_xamzdate(self):
        print '>>>>当前执行函数名称:',"%s.%s"%(self.__class__.__name__,get_current_function_name())
        #self.run_upload(self.filename,self.slicepath)
        #self.run_upload(self.filename,self.slicepath)
        uploadid = 'VIVKlbJlIz17TkWsrPT07Vcvd27GU7X3Ya3qt2Kqna47rpHd25o2aqvAsBUPKB8u1XEkPzKosiBmMBi8724S2A--'
        #打印当前执行函数的名称
        print '>>>>当前执行函数名称:',"%s.%s"%(self.__class__.__name__,get_current_function_name())
        resource= '/{0}/{1}?uploadId={2}'.format(self.bucket,self.filename,self.uploadid )
        #'/'+self.bucket+'/'+self.filename+'?uploads'
        print '>>>>resource信息：',resource
        contentType=''
        contentMD5=''
        method='DELETE'
        date=''
        xamzdate = self.date
        #token的算是 HMAC_SHA1算法，然后再次进行base64加密
        #HMAC运算利用哈希算法，以一个密钥和一个消息为输入，生成一个消息摘要作为输出
        token = generate_token(method,contentMD5,contentType,date,resource,self.S3_ACCESS_KEY_ID,self.S3_SECRET_ACCESS_KEY,xamzdate)
        print '>>>>鉴权后的token信息:\n',token
        headers = {'Authorization':token,'x-az-date':xamzdate}
        print '>>>>请求头部信息headers:\n',headers
        s3url = self.s3_url+resource
        print '>>>>请求的s3url:\n',s3url
        rq = requests.delete(url=s3url,headers=headers)
        status =  str(rq.status_code)
        print u'返回状态码 ',status
        print '响应头部信息：',rq.headers
        return_data = rq.text #接口返回的内容
        print return_data
        result = compare_xml_tag(return_data,
                                 status,
                                 code_message_info.status_403,
                                 code_message_info.code_403_AccessDenied,
                                 code_message_info.message_403_AccessDenied)
        self.assertEquals(result, "SUCCESS")


    '''
    @正常的请求:：header头部带有token，鉴权token缺少StringToSign
    '''
    def test_AbortMultipartUploadid_token_miss_StringToSign(self):
        print '>>>>当前执行函数名称:',"%s.%s"%(self.__class__.__name__,get_current_function_name())
        #self.run_upload(self.filename,self.slicepath)
        #self.run_upload(self.filename,self.slicepath)
        uploadid = 'VIVKlbJlIz17TkWsrPT07Vcvd27GU7X3Ya3qt2Kqna47rpHd25o2aqvAsBUPKB8u1XEkPzKosiBmMBi8724S2A--'
        #打印当前执行函数的名称
        print '>>>>当前执行函数名称:',"%s.%s"%(self.__class__.__name__,get_current_function_name())
        resource= '/{0}/{1}?uploadId={2}'.format(self.bucket,self.filename,self.uploadid )
        #'/'+self.bucket+'/'+self.filename+'?uploads'
        print '>>>>resource信息：',resource
        contentType=''
        contentMD5=''
        method='DELETE'
        date=self.date
        xamzdate = self.date
        #token的算是 HMAC_SHA1算法，然后再次进行base64加密
        #HMAC运算利用哈希算法，以一个密钥和一个消息为输入，生成一个消息摘要作为输出
        stringToSign = method + '\n' + contentMD5+'\n' + contentType+'\n' + date + '\n' + resource
        print u'鉴权信息stringToSign: ', stringToSign
        my_sign = hmac.new(self.S3_SECRET_ACCESS_KEY, stringToSign, sha1).digest()
        signature = base64.b64encode(my_sign)
        token = 'AWS' + ' ' + self.S3_ACCESS_KEY_ID + ':'
        print '>>>>鉴权后的token信息:\n',token
        headers = {'Authorization':token,'Date':date}
        print '>>>>请求头部信息headers:\n',headers
        s3url = self.s3_url+resource
        print '>>>>请求的s3url:\n',s3url
        rq = requests.delete(url=s3url,headers=headers)
        status =  str(rq.status_code)
        print u'返回状态码 ',status
        print '响应头部信息：',rq.headers
        return_data = rq.text #接口返回的内容
        print return_data
        result = compare_xml_tag(return_data,
                                 status,
                                 code_message_info.status_400,
                                 code_message_info.code_400_InvalidArgument,
                                 code_message_info.message_400_InvalidArgument)
        self.assertEquals(result, "SUCCESS")


    '''
    @正常的请求:：header头部带有token，StringToSign 鉴权中的token 缺少 'AWS'
    '''
    def test_AbortMultipartUploadid_token_miss_AWS(self):
        print '>>>>当前执行函数名称:',"%s.%s"%(self.__class__.__name__,get_current_function_name())
        #self.run_upload(self.filename,self.slicepath)
        #self.run_upload(self.filename,self.slicepath)
        uploadid = 'VIVKlbJlIz17TkWsrPT07Vcvd27GU7X3Ya3qt2Kqna47rpHd25o2aqvAsBUPKB8u1XEkPzKosiBmMBi8724S2A--'
        #打印当前执行函数的名称
        print '>>>>当前执行函数名称:',"%s.%s"%(self.__class__.__name__,get_current_function_name())
        resource= '/{0}/{1}?uploadId={2}'.format(self.bucket,self.filename,self.uploadid )
        #'/'+self.bucket+'/'+self.filename+'?uploads'
        print '>>>>resource信息：',resource
        contentType=''
        contentMD5=''
        method='DELETE'
        date=self.date
        xamzdate = self.date
        #token的算是 HMAC_SHA1算法，然后再次进行base64加密
        #HMAC运算利用哈希算法，以一个密钥和一个消息为输入，生成一个消息摘要作为输出
        stringToSign = method + '\n' + contentMD5+'\n' + contentType+'\n' + date + '\n' + resource
        print u'鉴权信息stringToSign: ', stringToSign
        my_sign = hmac.new(self.S3_SECRET_ACCESS_KEY, stringToSign, sha1).digest()
        signature = base64.b64encode(my_sign)
        token =  self.S3_ACCESS_KEY_ID + ':'+signature
        print '>>>>鉴权后的token信息:\n',token
        headers = {'Authorization':token,'Date':date}
        print '>>>>请求头部信息headers:\n',headers
        s3url = self.s3_url+resource
        print '>>>>请求的s3url:\n',s3url
        rq = requests.delete(url=s3url,headers=headers)
        status =  str(rq.status_code)
        print u'返回状态码 ',status
        print '响应头部信息：',rq.headers
        return_data = rq.text #接口返回的内容
        print return_data
        result = compare_xml_tag(return_data,
                                 status,
                                 code_message_info.status_400,
                                 code_message_info.code_400_InvalidArgument,
                                 code_message_info.message_400_InvalidArgument_2)
        self.assertEquals(result, "SUCCESS")

    '''
    @正常的请求:：header头部带有token，StringToSign 鉴权中token 中的‘AWS’ 被写成‘aw’
    '''
    def test_AbortMultipartUploadid_token_AWS_changeto_AS(self):
        print '>>>>当前执行函数名称:',"%s.%s"%(self.__class__.__name__,get_current_function_name())
        #self.run_upload(self.filename,self.slicepath)
        #self.run_upload(self.filename,self.slicepath)
        uploadid = 'VIVKlbJlIz17TkWsrPT07Vcvd27GU7X3Ya3qt2Kqna47rpHd25o2aqvAsBUPKB8u1XEkPzKosiBmMBi8724S2A--'
        #打印当前执行函数的名称
        print '>>>>当前执行函数名称:',"%s.%s"%(self.__class__.__name__,get_current_function_name())
        resource= '/{0}/{1}?uploadId={2}'.format(self.bucket,self.filename,self.uploadid )
        #'/'+self.bucket+'/'+self.filename+'?uploads'
        print '>>>>resource信息：',resource
        contentType=''
        contentMD5=''
        method='DELETE'
        date=self.date
        xamzdate = self.date
        #token的算是 HMAC_SHA1算法，然后再次进行base64加密
        #HMAC运算利用哈希算法，以一个密钥和一个消息为输入，生成一个消息摘要作为输出
        stringToSign = method + '\n' + contentMD5+'\n' + contentType+'\n' + date + '\n' + resource
        print u'鉴权信息stringToSign: ', stringToSign
        my_sign = hmac.new(self.S3_SECRET_ACCESS_KEY, stringToSign, sha1).digest()
        signature = base64.b64encode(my_sign)
        token = 'AS' + ' ' + self.S3_ACCESS_KEY_ID + ':'+signature
        print '>>>>鉴权后的token信息:\n',token
        headers = {'Authorization':token,'Date':date}
        print '>>>>请求头部信息headers:\n',headers
        s3url = self.s3_url+resource
        print '>>>>请求的s3url:\n',s3url
        rq = requests.delete(url=s3url,headers=headers)
        status =  str(rq.status_code)
        print u'返回状态码 ',status
        print '响应头部信息：',rq.headers
        return_data = rq.text #接口返回的内容
        print return_data
        result = compare_xml_tag(return_data,
                                 status,
                                 code_message_info.status_400,
                                 code_message_info.code_400_InvalidArgument,
                                 code_message_info.message_400_InvalidArgument_3)
        self.assertEquals(result, "SUCCESS")


    '''
    @正常的请求:：token中的WCS字段和ak之间只能有一个空格，否则报400 BadRequest
    '''
    def test_AbortMultipartUploadid_token_more_space(self):
        print '>>>>当前执行函数名称:',"%s.%s"%(self.__class__.__name__,get_current_function_name())
        #self.run_upload(self.filename,self.slicepath)
        #self.run_upload(self.filename,self.slicepath)
        uploadid = 'VIVKlbJlIz17TkWsrPT07Vcvd27GU7X3Ya3qt2Kqna47rpHd25o2aqvAsBUPKB8u1XEkPzKosiBmMBi8724S2A--'
        #打印当前执行函数的名称
        print '>>>>当前执行函数名称:',"%s.%s"%(self.__class__.__name__,get_current_function_name())
        resource= '/{0}/{1}?uploadId={2}'.format(self.bucket,self.filename,self.uploadid )
        #'/'+self.bucket+'/'+self.filename+'?uploads'
        print '>>>>resource信息：',resource
        contentType=''
        contentMD5=''
        method='DELETE'
        date=self.date
        xamzdate = self.date
        #token的算是 HMAC_SHA1算法，然后再次进行base64加密
        #HMAC运算利用哈希算法，以一个密钥和一个消息为输入，生成一个消息摘要作为输出
        stringToSign = method + '\n' + contentMD5+'\n' + contentType+'\n' + date + '\n' + resource
        print u'鉴权信息stringToSign: ', stringToSign
        my_sign = hmac.new(self.S3_SECRET_ACCESS_KEY, stringToSign, sha1).digest()
        signature = base64.b64encode(my_sign)
        token = 'AWS' + '   ' + self.S3_ACCESS_KEY_ID + ':'+signature
        print '>>>>鉴权后的token信息:\n',token
        headers = {'Authorization':token,'Date':date}
        print '>>>>请求头部信息headers:\n',headers
        s3url = self.s3_url+resource
        print '>>>>请求的s3url:\n',s3url
        rq = requests.delete(url=s3url,headers=headers)
        status =  str(rq.status_code)
        print u'返回状态码 ',status
        print '响应头部信息：',rq.headers
        return_data = rq.text #接口返回的内容
        print return_data
        result = compare_xml_tag(return_data,
                                 status,
                                 code_message_info.status_400,
                                 code_message_info.code_400_InvalidArgument,
                                 code_message_info.message_400_InvalidArgument_2)
        self.assertEquals(result, "SUCCESS")

    '''
    @正常的请求:：header 头部的"Authorization" 值被写成 "Authzation"
    '''
    def test_AbortMultipartUploadid_header_Authorization_changeto_Authzation(self):
        print '>>>>当前执行函数名称:',"%s.%s"%(self.__class__.__name__,get_current_function_name())
        #self.run_upload(self.filename,self.slicepath)
        #self.run_upload(self.filename,self.slicepath)
        uploadid = 'VIVKlbJlIz17TkWsrPT07Vcvd27GU7X3Ya3qt2Kqna47rpHd25o2aqvAsBUPKB8u1XEkPzKosiBmMBi8724S2A--'
        #打印当前执行函数的名称
        print '>>>>当前执行函数名称:',"%s.%s"%(self.__class__.__name__,get_current_function_name())
        resource= '/{0}/{1}?uploadId={2}'.format(self.bucket,self.filename,self.uploadid )
        #'/'+self.bucket+'/'+self.filename+'?uploads'
        print '>>>>resource信息：',resource
        contentType=''
        contentMD5=''
        method='DELETE'
        date=self.date
        xamzdate = self.date
        #token的算是 HMAC_SHA1算法，然后再次进行base64加密
        #HMAC运算利用哈希算法，以一个密钥和一个消息为输入，生成一个消息摘要作为输出
        stringToSign = method + '\n' + contentMD5+'\n' + contentType+'\n' + date + '\n' + resource
        print u'鉴权信息stringToSign: ', stringToSign
        my_sign = hmac.new(self.S3_SECRET_ACCESS_KEY, stringToSign, sha1).digest()
        signature = base64.b64encode(my_sign)
        token = 'AWS' + ' ' + self.S3_ACCESS_KEY_ID + ':'+signature
        print '>>>>鉴权后的token信息:\n',token
        headers = {'Authzation':token,'Date':date}
        print '>>>>请求头部信息headers:\n',headers
        s3url = self.s3_url+resource
        print '>>>>请求的s3url:\n',s3url
        rq = requests.delete(url=s3url,headers=headers)
        status =  str(rq.status_code)
        print u'返回状态码 ',status
        print '响应头部信息：',rq.headers
        return_data = rq.text #接口返回的内容
        print return_data
        result = compare_xml_tag(return_data,
                                 status,
                                 code_message_info.status_403,
                                 code_message_info.code_403_AccessDenied,
                                 code_message_info.message_403_AccessDenied_2)
        self.assertEquals(result, "SUCCESS")



if __name__ == '__main__':
    aa = myUI_AbortMultipartuploadid()
    #aa.InitialMultipartUpload_ok('test.mp4')
    aa.test_AbortMultipartUploadid_header_contenttype_ischinese()
