/* Copyright (C) 2015~ Hyundai Heavy Industries. All Rights Reserved.
 *
 * This software is the confidential and proprietary information of Hyundai Heavy Industries
 * You shall not disclose such Confidential Information and shall use it only 
 * in accordance with the terms of the license agreement
 * you entered into with Hyundai Heavy Industries.
 *
 * Revision History
 * Author			Date				Description
 * ---------------	----------------	------------
 * Sang-cheon Park	2015. 10. 13.		First Draft.
 */
package com.hhi.bigdata.platform.push.service.app;

/**
 * <pre>
 * WSO2 API Manager's access info
 * </pre>
 * @author Sang-cheon Park
 * @version 1.0
 */
public class APIInfo {

	/** username */
	private String username;
	/** password */
	private String password;
	/** consumer key */
	private String consumerKey;
	/** consumer secret */
	private String consumerSecret;

	/**
	 * @return the username
	 */
	public String getUsername() {
		return username;
	}

	/**
	 * @param username the username to set
	 */
	public void setUsername(String username) {
		this.username = username;
	}

	/**
	 * @return the password
	 */
	public String getPassword() {
		return password;
	}

	/**
	 * @param password the password to set
	 */
	public void setPassword(String password) {
		this.password = password;
	}

	/**
	 * @return the consumerKey
	 */
	public String getConsumerKey() {
		return consumerKey;
	}

	/**
	 * @param consumerKey the consumerKey to set
	 */
	public void setConsumerKey(String consumerKey) {
		this.consumerKey = consumerKey;
	}

	/**
	 * @return the consumerSecret
	 */
	public String getConsumerSecret() {
		return consumerSecret;
	}

	/**
	 * @param consumerSecret the consumerSecret to set
	 */
	public void setConsumerSecret(String consumerSecret) {
		this.consumerSecret = consumerSecret;
	}
	
	@Override
	public String toString() {
		return "[username : " + username +
				", password : " + password +
				", consumerKey : " + consumerKey +
				", consumerSecret : " + consumerSecret + "]";
	}
}
//end of APIInfo.java