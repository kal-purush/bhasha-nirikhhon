package com.api.Actions;

import java.io.FileNotFoundException;

import com.api.EnvVariables.EnvConstants;
import com.api.EnvVariables.EnvVariables;
import com.api.ReusableUtils.Reusable_CRUD_Operations;

import io.restassured.response.Response;
import io.restassured.specification.RequestSpecification;

public class UserLogout_Actions {
	
	Reusable_CRUD_Operations restUtil = new Reusable_CRUD_Operations();
	Response response;
	RequestSpecification reqSpec;
	String token;
	/*
	 * =================================Building request
	 * specification======================================
	 */

	public RequestSpecification buildRequest() throws FileNotFoundException {
		RequestSpecification reqSpec;
		reqSpec = restUtil.getRequestSpec();
		return reqSpec;
	}
	
	/*
	 * =================================Logout request
	 * ======================================
	 */
	public Response userLogout(RequestSpecification reqSpec,String currentTag) throws FileNotFoundException {
		
		switch (currentTag) {
		case "@Logout1":
			 token =EnvVariables.token;
			response = restUtil.retrieve(reqSpec,token, EnvConstants.logout_Endpoint);
			System.out.println("actualMessage is :"+response.asString());
			
			break;
		case "@InvalidMethodLogout2":
			token =EnvVariables.token;
			response = restUtil.postLogout(reqSpec,token, EnvConstants.logout_Endpoint);
			System.out.println("actualMessage is :"+response.jsonPath().getString("message"));
			break;
		case "@LogoutNoAuth3":
			response =restUtil.retrieve(reqSpec, EnvConstants.logout_Endpoint);
			System.out.println("actualMessage is :"+response.jsonPath().getString("message"));
			break;
		default:
			throw new RuntimeException("no matching tag :" + currentTag);
				
	}return response;
	
}
}