package com.zcc.login.common.exception;

import com.zcc.login.common.constant.ErrorCodeEnum;

/**
 * Created by ZhangChicheng on 2017/11/22.
 */
public class CommonException extends Exception{
	protected int errorCode;
	protected String errorMsg;

	public CommonException(){}

	public CommonException(ErrorCodeEnum errorCodeEnum){
		this.errorCode = errorCodeEnum.getErrorCode();
		this.errorMsg = errorCodeEnum.getErrorMsg();
	}

	public CommonException(int errorCode, String errorMsg) {
		this.errorCode = errorCode;
		this.errorMsg = errorMsg;
	}

	public int getErrorCode() {
		return errorCode;
	}

	public void setErrorCode(int errorCode) {
		this.errorCode = errorCode;
	}

	public String getErrorMsg() {
		return errorMsg;
	}

	public void setErrorMsg(String errorMsg) {
		this.errorMsg = errorMsg;
	}
}