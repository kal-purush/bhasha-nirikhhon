package com.mk.projects.mail;

public class EmailResponse {

	private boolean successful;
	private String reasonCode;
	private String errorStack;
	
	public EmailResponse (boolean successful) {
		this.successful = successful;
	}
	
	public EmailResponse (String reasonCode, String errorStack) {
		this.successful = false;
		this.reasonCode = reasonCode;
		this.errorStack = errorStack;
	}
	
	public boolean isSuccessful() {
		return successful;
	}
	public void setSuccessful(boolean successful) {
		this.successful = successful;
	}
	public String getReasonCode() {
		return reasonCode;
	}
	public void setReasonCode(String reasonCode) {
		this.reasonCode = reasonCode;
	}
	public String getErrorStack() {
		return errorStack;
	}
	public void setErrorStack(String errorStack) {
		this.errorStack = errorStack;
	}
}