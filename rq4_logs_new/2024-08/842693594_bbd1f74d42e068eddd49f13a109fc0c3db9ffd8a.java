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


public class Morbidity_Get_Actions {	

	Reusable_CRUD_Operations restUtil= new Reusable_CRUD_Operations();
	Response response;
	public String requestBody = "";
/*=================================Building request specification======================================*/

public RequestSpecification buildRequest() throws FileNotFoundException
{
	RequestSpecification reqSpec;
	reqSpec = restUtil.getRequestSpec();
	return reqSpec;
}

public Response RetrieveAdminMorbidity(RequestSpecification reqSpec)
  {
      response = restUtil.retrieve(reqSpec, EnvVariables.token, EnvConstants.morbidity_Endpoint);
	System.out.println("The Admin token for Morbidity stored in EnvVariables.token is "+ EnvVariables.token);
	return response;
  }

public Response GetMorbiditywithNoAuth(RequestSpecification reqSpec)
{
    response = restUtil.retrieve(reqSpec, EnvConstants.morbidity_Endpoint);
    System.out.println("The Admin token for Morbidity stored in EnvVariables.token is "+ EnvVariables.token);
	return response;
}

public Response RetrieveDieticianMorbidity(RequestSpecification reqSpec)
{
    response = restUtil.retrieve(reqSpec, EnvVariables.Diet_token, EnvConstants.morbidity_Endpoint);
	System.out.println("The Dietician token for Morbidity stored in EnvVariables.Diet_token is "+ EnvVariables.Diet_token);
	return response;
}

}


 	