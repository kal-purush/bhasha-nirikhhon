package com.yundian.celebrity.helper;

import android.content.Context;
import android.text.Editable;
import android.text.TextUtils;
import android.text.TextWatcher;
import android.view.View;
import android.widget.EditText;

import com.yundian.celebrity.utils.StringUtil;
import com.yundian.celebrity.utils.ToastUtils;
import com.yundian.celebrity.utils.Utils;
import com.yundian.celebrity.widget.CheckException;
import com.yundian.celebrity.widget.SimpleTextWatcher;
import com.yundian.celebrity.widget.WPEditText;


/**
 * @author : created by chuangWu
 * @version : 0.01
 * @email : chuangwu127@gmail.com
 * @created time : 2016-11-23 10:02
 * @description : none
 * @for your attention : none
 * @revise : none
 */
public class CheckInfoHelper {

    private static final String EMPEY_USERNAME = "用户名不能为空";
    private static final String LENGTH_MAX_USERNAME = "用户名不能超过50个字符";
    private static final String LENGTH_MIN_PASSWORD = "密码不能小于6个字符";
    private static final String LENGTH_MAX_PASSWORD = "密码不能大于16个字符";
    private static final String EMPEY_PASSWORD = "密码不能为空";
    private static final String ERROR_MOBILE = "手机号码不正确";
    private static final String ERROR_MOBILE2 = "两次手机号码不一致";
    private static final String ERROR_PWD2 = "两次密码输入不一致";
    private static final String ERROR_NUM2 = "两次号码不一致";
    private static final String EMPEY_VERIFY_CODE = "短信验证码不能为空";
    private static final String LENGTH_MAX_VERIFY = "验证码不能大于10个字符";

    public boolean checkUsername(String username, CheckException exception) {
        if (exception == null) {
            exception = new CheckException();
        }
        if (username.length() > 50) {
            exception.setErrorMsg(LENGTH_MAX_USERNAME);
            return false;
        }
        return true;
    }

    public boolean checkPassword(String password, CheckException exception) {
        if (exception == null) {
            exception = new CheckException();
        }

        if (password.length() < 6) {  //临时修改
            exception.setErrorMsg(LENGTH_MIN_PASSWORD);
            return false;
        } else if (password.length() > 16) {
            exception.setErrorMsg(LENGTH_MAX_PASSWORD);
            return false;
        }
        return true;
    }

    public boolean checkPassword2(String password, String password2, CheckException exception) {
        if (exception == null) {
            exception = new CheckException();
        }
        if (!password.equals(password2)) {
            exception.setErrorMsg(ERROR_PWD2);
            return false;
        }
        return true;
    }

    public boolean checkNumber2(String number, String number1, CheckException exception) {
        if (exception == null) {
            exception = new CheckException();
        }
        if (!number.equals(number1)) {
            exception.setErrorMsg(ERROR_NUM2);
            return false;
        }
        return true;
    }

    public boolean checkMobile(String mobile, CheckException exception) {
        if (exception == null) {
            exception = new CheckException();
        }
        if (!Utils.isMobile(mobile)) {
            exception.setErrorMsg(ERROR_MOBILE);
            return false;
        }
        return true;
    }


    public boolean checkMobile2(String mobile, String mobile2, CheckException exception) {
        if (exception == null) {
            exception = new CheckException();
        }
        if (!mobile2.equals(mobile)) {
            exception.setErrorMsg(ERROR_MOBILE2);
            return false;
        }
        return true;
    }

}