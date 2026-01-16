package org.gaurav.restws.exception;

import javax.ws.rs.core.Response.Status;

public class MyException extends Exception {

	private Status errCode;
	
	public MyException() {
		super();
	}

	public MyException(String message) {
		super(message);
	}
	
	public MyException(Status errCode, String message) {
		super(message);
		this.errCode=errCode;
	} 

	public MyException(Status errCode) {
		super();
		this.errCode=errCode;
	}

	public Status getErrCode() {
		return errCode;
	}


	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	

}