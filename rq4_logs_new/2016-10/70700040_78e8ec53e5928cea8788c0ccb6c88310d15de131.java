package com.lovebaby.uploadfileprj;

import com.alibaba.fastjson.JSONObject;

/**
 * 同步的网络请求返回数据
 * @author chengr
 *
 */
public class SynchNetResponseData {
	
	private boolean isSuccess;//是否成功
	private int errorCode;//错误时的错误编码
	private JSONObject responseData;//成功时返回的数据
	private String errorMsg;//错误信息
	
	public boolean isSuccess() {
		return isSuccess;
	}
	public void setSuccess(boolean isSuccess) {
		this.isSuccess = isSuccess;
	}
	public int getErrorCode() {
		return errorCode;
	}
	public void setErrorCode(int errorCode) {
		this.errorCode = errorCode;
	}
	public JSONObject getResponseData() {
		return responseData;
	}
	public void setResponseData(JSONObject responseData) {
		this.responseData = responseData;
	}
	public String getErrorMsg() {
		return errorMsg;
	}
	public void setErrorMsg(String errorMsg) {
		this.errorMsg = errorMsg;
	}
}