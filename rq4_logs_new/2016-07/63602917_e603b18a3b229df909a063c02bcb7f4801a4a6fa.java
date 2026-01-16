package com.mk.projects.mail;

abstract class AbstractEmail {	
	
	/**
	 * 
	 * @param emailTo
	 * @param subject
	 * @param body
	 */
	public abstract EmailResponse sendEmail (String emailTo, String subject, String body);
}