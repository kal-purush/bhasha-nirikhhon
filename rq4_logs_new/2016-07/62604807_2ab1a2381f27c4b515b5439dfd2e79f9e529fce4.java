package com.icip.pc.bean;

import com.icip.framework.common.data.message.MessageHeader;

public class RequestHeader implements MessageHeader {
	private static final long serialVersionUID = 8134488627956920361L;
	private String serviceCode;
	private String custNo;
	private String companyID;
//	private String systemCode;
	private String softTerminalVersion;
	private String interfaceVersion;
	private String softTerminalId;
	private String deviceOs;
	private String deviceOsVersion;
	private String productId;
	private String deviceId;
	private String conversationId;
	private String accessToken;
	private String systemDate;
	private String transCode;
	private String cid;

	public String getCid() {
		return cid;
	}

	public void setCid(String cid) {
		this.cid = cid;
	}

	public String getServiceCode() {
		return this.serviceCode;
	}

	public void setServiceCode(String serviceCode) {
		this.serviceCode = serviceCode;
	}

	public String getCompanyID() {
		return this.companyID;
	}

	public void setCompanyID(String companyID) {
		this.companyID = companyID;
	}

//	public String getSystemCode() {
//		return this.systemCode;
//	}

//	public void setSystemCode(String systemCode) {
//		this.systemCode = systemCode;
//	}

	public String getSoftTerminalVersion() {
		return this.softTerminalVersion;
	}

	public void setSoftTerminalVersion(String softTerminalVersion) {
		this.softTerminalVersion = softTerminalVersion;
	}

	public String getInterfaceVersion() {
		return this.interfaceVersion;
	}

	public void setInterfaceVersion(String interfaceVersion) {
		this.interfaceVersion = interfaceVersion;
	}

	public String getSoftTerminalId() {
		return this.softTerminalId;
	}

	public void setSoftTerminalId(String softTerminalId) {
		this.softTerminalId = softTerminalId;
	}

	public String getDeviceOs() {
		return this.deviceOs;
	}

	public void setDeviceOs(String deviceOs) {
		this.deviceOs = deviceOs;
	}

	public String getDeviceOsVersion() {
		return this.deviceOsVersion;
	}

	public void setDeviceOsVersion(String deviceOsVersion) {
		this.deviceOsVersion = deviceOsVersion;
	}

	public String getProductId() {
		return this.productId;
	}

	public void setProductId(String productId) {
		this.productId = productId;
	}

	public String getConversationId() {
		return this.conversationId;
	}

	public void setConversationId(String conversationId) {
		this.conversationId = conversationId;
	}

	public String getAccessToken() {
		return this.accessToken;
	}

	public void setAccessToken(String accessToken) {
		this.accessToken = accessToken;
	}

	public String getSystemDate() {
		return this.systemDate;
	}

	public void setSystemDate(String systemDate) {
		this.systemDate = systemDate;
	}

	public String getCustNo() {
		return this.custNo;
	}

	public void setCustNo(String custNo) {
		this.custNo = custNo;
	}

	public String getDeviceId() {
		return this.deviceId;
	}

	public void setDeviceId(String deviceId) {
		this.deviceId = deviceId;
	}

	public String getTransCode() {
		return transCode;
	}

	public void setTransCode(String transCode) {
		this.transCode = transCode;
	}
}