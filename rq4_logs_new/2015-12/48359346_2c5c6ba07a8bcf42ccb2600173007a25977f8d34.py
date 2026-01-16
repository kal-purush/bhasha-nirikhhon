#!coding:utf8
from hashlib import md5
import urllib2
import random
import json
# SDK_version = "3.0.1"
# Python_version = 2.7.6


class GeetestLib(object):
    """docstring for gt"""

    def __init__(self, id, key):
        self.private_key = key   #Private Key
        self.captcha_ID = id       #Public Key
        self.py_version = "2.7.6"
        self.challenge = ""

        host_port = 80
        self.base_url = "api.geetest.com"
        self.api_url = "http://" + self.base_url
        self.host = host_port
        
        self.success_res = "success"
        self.fail_res = "fail"
        
        self.forbidden_res = "forbidden"
        self.validate_logpath = ""
        self.debug_code = True


        self.fn_challenge = "geetest_challenge"
        self.fn_validate = "geetest_validate"
        self.fn_seccode = "geetest_seccode"

        self.gt_sessionkey = "geetest"
        self.gt_server_status_session_key = "gt_server_status"

        self.product_type = "embed"
        self.popup_btn_ID = ""

        self.https = False



    def pre_process(self):
        """
        验证初始化预处理
        :return Boolean:
        """ 
        if self.register():
            return True
        else:
            return False

    def register(self):
        path = "/register.php"
        host = self.host
        captcha_ID = self.captcha_ID
        if self.captcha_ID == None:
            return False
        else:
            challenge=self.register_challenge()
            if len(challenge) == 32:
                self.challenge = challenge
                return True
            else:
                return False

    def get_fail_pre_process_res(self):
        """
        预处理失败后的返回格式串
        :return Json字符串:
        """ 
        rnd1 = long(round(random.random()*100))
        rnd2 = long(round(random.random()*100))
        md5_str1 = md5_encode(str(rnd1))
        md5_str2 = md5_encode(str(rnd2))
        challenge = md5_str1 + md5_str2[0:2]
        string_format = json.dumps({'success': 0, 'gt':self.captcha_ID ,'challenge': self.challenge})
        return string_format

    def get_success_pre_process_res(self):
        """
        预处理成功后的标准串
        :return Json字符串:
        """ 
        string_format = json.dumps({'success': 1, 'gt':self.captcha_ID ,'challenge': self.challenge})
        return string_format

    def register_challenge(self):
        api_reg = "http://api.geetest.com/register.php?"
        reg_url = api_reg + "gt=%s"%self.captcha_ID
        try:
            ret_string = urllib2.urlopen(reg_url, timeout=2).read()
        except:
            ret_string = ""
        return ret_string

    def post_validate(self, challenge, validate, seccode):
        apiserver = "http://api.geetest.com/validate.php"
        if validate == self.md5_encode(self.private_key + 'geetest' + challenge):
            query = 'seccode=' + seccode + "&sdk=python_" + self.py_version
            print query
            backinfo = self.post_values(apiserver, query)
            if backinfo == self.md5_encode(seccode):
                return 1
            else:
                return 0
        else:
            return 0

    def post_values(self, apiserver, data):
        """
        向gt-server发起二次验证请求
        :param host:
        :param path:
        :param data:
        :return:
        """
        req = urllib2.Request(apiserver)
        opener = urllib2.build_opener(urllib2.HTTPCookieProcessor())
        response = opener.open(req, data)
        backinfo = response.read()
        return backinfo

    def check_result_by_private(self, orgin, validate):
        """
        二次验证先验判断，判断validate是否与privatekey challenge 吻合
        :param origin:
        :param validate:
        :returns Boolean 判断结果:
        """
        encodeStr = md5_encode(privateKey + "geetest" + origin)
        if validate == encodeStr:
            return True
        else:
            return False

    def enhenced_validate_request(self, request):
        """
        向gt-server进行二次验证
        :param request:
        :returns String 二次验证结果:
        """
        if  not  request_is_legal(request):
            return self.fail_res     
        path = "/validate.php"
        challenge = request.form['geetest_challenge']
        validate = request.form['geetest_validate']
        seccode = request.form['geetest_seccode']
        if  validate.Length > 0 and check_result_by_private(challenge, validate):
            query = "seccode=" + seccode + "&sdk=csharp_" + self.version
            response = ""
            try:
                response = post_validate(self.host, path, query)
            except:
                print "postValidate error"
            if response == md5_encode(seccode):
                return self.successResult
        return self.fail_res

    def request_is_legal(self, request):
        """
        判断请求是否合法
        :return Boolean:
        """
        challenge = request.form['geetest_challenge']
        validate = request.form['geetest_validate']
        seccode = request.form['geetest_seccode']
        if self.str_null(challenge) or self.str_null(validate) or  self.str_null(seccode):
            return False
        else :
            return True

    def failback_validate_request(self, request):
        """
        failback模式的验证方式
        :param request:
        :return 验证结果:
        """
        if not self.request_is_legal(request):
            return self.fail_res
        challenge = request.form['geetest_challenge']
        validate = request.form['geetest_validate']
        seccode = request.form['geetest_seccode']
        if not challenge == self.challenge:
            return self.fail_res
        validate_str = validate.split('_')
        encode_ans = validate_str[0]
        encode_full_bg_img_index = validate_str[1]
        encode_img_grp_index = validate_str[2]
        decode_ans = decode_response(self.challenge, encode_ans)
        decode_full_bg_img_index = decode_response(self.challenge, encode_full_bg_img_index)
        decode_img_grp_index = decode_response(self.challenge, encode_img_grp_index)
        validate_result = validate_fail_image(decode_ans, decode_full_bg_img_index, decode_img_grp_index)
        if not validate_result == self.fail_res:
            rand1 = get_random_num()
            md5Str = md5_encode(rand1 + "")
            self.challenge = md5Str
        return validate_result

    def validate_fail_image(self, ans, full_bg_index , img_grp_index):
        """
        failback模式下，简单判断轨迹是否通过
        :param ="ans"答案位置:
        :param ="full_bg_index":
        :param ="img_grp_index":
        :return 轨迹验证结果:
        """
        import string
        thread = 3 
        full_bg_name = md5_encode(full_bg_index + "")[0, 10]
        bg_name = md5_encode(img_grp_index + "")[10, 10]
        answer_decode = ""
        for i in range(0,9):
            if i % 2 == 0:
                answer_decode += full_bg_name[i]
            elif i % 2 == 1:
                answer_decode += bg_name[i]
        x_decode = answer_decode[4]
        x_int = string.atoi(x_decode, 16)
        result = x_int % 200
        if result < 40:
            result = 40
        if abs(ans - result) < thread:
            return self.success_res
        else:
            return self.fail_res

    def set_gtserver_session(self, session, status_code):
        """
       设置极验服务器的gt-server状态值
        :param session:
        :param status_code //gt-server状态值,0表示不正常，1表示正常:
        """
        session["gt_server_status"]=status_code

    def  get_gtserver_session(self, session):
        """
        获取gt-server状态值,0表示不正常，1表示正常
        :param session:
        :return:
        """
        status_code = int(session['gt_server_status'])
        return status_code

    def md5_encode(self, values):
    	"""
    	md5编码
    	"""
        m = hashlib.md5()
        m.update(values)
        return m.hexdigest()


    def decode_rand_base(self, challenge): 
        """
        输入的两位的随机数字,解码出偏移量
        :param challenge:
        :return: 
        """     
        str_base = challenge[32:34]
        i = 0
        temp_array = []
        while i < len(str_base):
            temp_char = str_base[i]
            temp_Ascii = ord(temp_char)
            result = temp_Ascii - 87 if temp_Ascii > 57 else temp_Ascii - 48
            temp_array.append(result)
            i += 1
        decode_res = temp_array[1]*36 + temp_array[1]
        return decode_res


    def decode_response(self, challenge, userresponse):
        """
        Use challenge&userresponse to decode response
        :param challenge:
        :param string:
        :return: 
        """
        if len(userresponse) > 100:
            return 0
        shuzi = (1, 2, 5, 10, 50)
        chongfu = []
        key = {}
        count = 0
        for i in challenge:
            if i in chongfu:
                continue
            else:
                value = shuzi[count % 5]   
                chongfu.append(i)
                count += 1
                key.update({i: value})
        res = 0
        for i in userresponse:
            res += key.get(i, 0)
        return res

    def get_random_num():
        rand_num = random.random()*100
        return rand_num

    def str_null(self,str1):
        """
        字符串判断，若为空返还True
        """
        if not str1.strip():
            return False
        else:
            return True

    def test(self):
        print self.fail_res

