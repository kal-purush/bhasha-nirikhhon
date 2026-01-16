package com.api.Actions;

import java.io.FileNotFoundException;
import java.time.LocalDate;
import java.time.Period;
import java.util.Random;

import org.apache.commons.lang3.RandomStringUtils;

import com.api.EnvVariables.EnvConstants;
import com.api.EnvVariables.EnvVariables;
import com.api.ReusableUtils.Reusable_CRUD_Operations;
import com.google.gson.Gson;
import com.google.gson.JsonObject;

import io.restassured.response.Response;
import io.restassured.specification.RequestSpecification;

public class Dietician_Post_Get_Actions {
	
	Reusable_CRUD_Operations restUtil= new Reusable_CRUD_Operations();
	private String requestBody = "";
	String token;
	
	/*=================================Building request specification======================================*/
	
	public RequestSpecification buildRequest() throws FileNotFoundException
	{
		RequestSpecification reqSpec;
		reqSpec = restUtil.getRequestSpec();
		return reqSpec;
	}
	 	

	/*============================post request to create a dietician===============================================*/

	public Response createDietician(RequestSpecification reqSpec) throws FileNotFoundException {
		
		String ContactNumber = RandomStringUtils.randomNumeric(10);
		String DateOfBirth= (LocalDate.now().minus(Period.ofDays((new Random().nextInt(365 * 70))))).toString();
		String Education = RandomStringUtils.randomAlphabetic(10);
		String Email = RandomStringUtils.randomAlphanumeric(6) + "@ninja.com";
		String Firstname = RandomStringUtils.randomAlphabetic(10);
		String HospitalCity = RandomStringUtils.randomAlphabetic(10);
		String HospitalName = RandomStringUtils.randomAlphabetic(10);
		String HospitalPincode = RandomStringUtils.randomNumeric(6);
		String HospitalStreet = RandomStringUtils.randomAlphabetic(10);
		String Lastname = RandomStringUtils.randomAlphabetic(10);
		
		 // Construct JSON payload using Gson
        JsonObject json = new JsonObject();
        json.addProperty("ContactNumber", ContactNumber);
        json.addProperty("DateOfBirth", DateOfBirth);
        json.addProperty("Education", Education);
        json.addProperty("Email", Email);
        json.addProperty("Firstname", Firstname);
        json.addProperty("HospitalCity", HospitalCity);
        json.addProperty("HospitalName", HospitalName);
        json.addProperty("HospitalPincode", HospitalPincode);
        json.addProperty("HospitalStreet", HospitalStreet);
        json.addProperty("Lastname", Lastname);

        Gson gson = new Gson();
        requestBody = gson.toJson(json);
		
		System.out.println("Login request Body is : "+requestBody);
		Response response = restUtil.create(reqSpec, EnvVariables.token, requestBody, EnvConstants.createDietician_Endpoint);
		
		return response;
	}


	/*===========================Storing the required response data in Env variables==============================================*/

	public void storeDieticianInfo(Response response)
	{
		//System.out.println("response sending from actions "+response.asPrettyString());
		String id= restUtil.extractStringFromResponse(response, "id");
		String loginPassword= restUtil.extractStringFromResponse(response, "loginPassword");
		String email= restUtil.extractStringFromResponse(response, "Email");
		System.out.println("The token from the response is "+ id);
		System.out.println("The token from the response is "+ loginPassword);
		System.out.println("The token from the response is "+ email);
		EnvVariables.dietician1_ID= id;
		EnvVariables.dietician1_Email = email;
		EnvVariables.dietician1_loginPassword = loginPassword;
		System.out.println("The token stored in EnvVariables.token is "+ EnvVariables.dietician1_ID);
		System.out.println("The token stored in EnvVariables.dietician1_Email is "+ EnvVariables.dietician1_Email);
		System.out.println("The token stored in EnvVariables.dietician1_loginPassword is "+ EnvVariables.dietician1_loginPassword);
	}

}