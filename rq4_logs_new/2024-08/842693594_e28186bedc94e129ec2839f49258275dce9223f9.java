package com.api.Actions;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import com.api.EnvVariables.EnvConstants;
import com.api.ReusableUtils.Reusable_CRUD_Operations;
import com.google.gson.Gson;
import com.google.gson.JsonObject;

import io.restassured.response.Response;
import io.restassured.specification.RequestSpecification;

public class UserLogin_Actions {
	
Reusable_CRUD_Operations restUtil= new Reusable_CRUD_Operations();
private String requestBody = "";
	
	public void readProperties()
	{
	Properties prop= new Properties();
	InputStream input=null;
	
	
	try {
		//input= new FileInputStream("config.properties");
		  input = getClass().getClassLoader().getResourceAsStream("Configs/config.properties");
		  if (input == null) {
              System.out.println("Sorry, unable to find Configs/config.properties");
              return;
		  }
		prop.load(input);
		String password= prop.getProperty("password");
		String userLoginEmail= prop.getProperty("userLoginEmail");
		
		//System.out.println("password is"+ password);
		//System.out.println("userLoginEmail is"+ userLoginEmail);
		
		 // Construct JSON payload using Gson
        JsonObject json = new JsonObject();
        json.addProperty("password", password);
        json.addProperty("userLoginEmail", userLoginEmail);

        Gson gson = new Gson();
        requestBody = gson.toJson(json);
		
		
	}
	catch(IOException ex)
	{
		ex.printStackTrace();
	}
	finally
	{
		 if (input != null) {
             try {
                 input.close();
             } catch (IOException e) {
                 e.printStackTrace();
             }
         }
	}
}
	
public RequestSpecification buildRequest() throws FileNotFoundException
{
	RequestSpecification reqSpec;
	reqSpec = restUtil.getRequestSpec();
	return reqSpec;
}


public Response loginToGetAuthorized_User(RequestSpecification reqSpec) throws FileNotFoundException {
	readProperties();
	System.out.println("Login request Body is : "+requestBody);
	Response response = restUtil.create(reqSpec,requestBody, EnvConstants.login_Endpoint);
	
	return response;
}
}