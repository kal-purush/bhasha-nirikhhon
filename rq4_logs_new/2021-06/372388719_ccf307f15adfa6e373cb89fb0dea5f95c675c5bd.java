package com.org.loan.config;
import org.springframework.http.*;

public enum CoreError {
	SUCCESS(0, HttpStatus.OK),
	UNKNOWN_ERROR(2000, HttpStatus.INTERNAL_SERVER_ERROR),
	MESSAGING_ERROR(2100, HttpStatus.INTERNAL_SERVER_ERROR),
	REST_CLIENT_ERROR(2150, HttpStatus.INTERNAL_SERVER_ERROR),
	DB_ERROR(2250, HttpStatus.INTERNAL_SERVER_ERROR),
	REQUEST_ERROR(3000, HttpStatus.BAD_REQUEST);
	
	private int code;
	private HttpStatus status;
	
	
	private CoreError(int code, HttpStatus status) {
		this.code = code;
		this.status = status;
	}
	public int getCode() {
		return code;
	}
	public void setCode(int code) {
		this.code = code;
	}
	public HttpStatus getStatus() {
		return status;
	}
	public void setStatus(HttpStatus status) {
		this.status = status;
	}
	
	
}