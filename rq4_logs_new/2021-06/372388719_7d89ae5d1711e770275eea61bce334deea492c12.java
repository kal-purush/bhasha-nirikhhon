package com.org.user.config;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ResponseEnvelope<T> {
	private int errorCode;
	private String errorDescription;
	private String errorMessage;
	private T response;
	
	public ResponseEnvelope (
			@JsonProperty("errorCode") int  errorCode,
			@JsonProperty("errorDescription") String errorDescription,
			@JsonProperty("errorMessage") String errorMessage,
			@JsonProperty("response") T response) {
		this.errorCode = errorCode;
		this.errorDescription = errorDescription;
		this.errorMessage = errorMessage;
		this.response = response;
	}

//	public ResponseEnvelope(CoreError error, String errorMessage, T t) {
//		
//	}
}