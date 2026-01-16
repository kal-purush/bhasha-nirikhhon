/**
 * BaasJam -- Backend-as-a-service
 * 
 * Created by: Hongyi Zhang
 * Created Date: 2015/12/04
 * 
 * Functions:
 * This is the base class of all Restful Controller.
 * It contains some common functions for controllers.
 */

package com.baasjam.application.controller;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import com.baasjam.platform.web.response.BJHttpStatus;
import com.baasjam.platform.web.response.BJResponse;

public abstract class BJController {

	/**
	 * @param response
	 * @param status
	 * @return JSON serialized model, and status with message
	 */
	protected ResponseEntity<BJResponse> ok(BJResponse response,
			BJHttpStatus status) {
		response.setStatus(status);
		return new ResponseEntity<BJResponse>(response, HttpStatus.OK);
	}

	/**
	 * OK response with empty model
	 * 
	 * @return
	 */
	protected ResponseEntity<BJResponse> ok() {
		return new ResponseEntity<BJResponse>(HttpStatus.OK);
	}

	/**
	 * Bad request
	 * 
	 * @return
	 */
	protected ResponseEntity<BJResponse> badRequest() {
		return new ResponseEntity<BJResponse>(HttpStatus.BAD_REQUEST);
	}

}