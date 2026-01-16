package com.api.StepDefs;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.poi.openxml4j.exceptions.InvalidFormatException;

import com.api.Actions.UserLogin_Actions;
import com.api.ReusableUtils.API_BaseSetUp_Validations;

import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import io.restassured.response.Response;
import io.restassured.specification.RequestSpecification;

public class PatientLogin {
	UserLogin_Actions actionsLogin= new UserLogin_Actions();
	RequestSpecification reqSpec;
	Response response;
	int statusCode;
	long expectedResponseTime;
	String expectedContentType;
	String currentTag;
@Given("User creates Post request with request body for patient Login")
public void user_creates_post_request_with_request_body_for_patient_login() {
	try {
		reqSpec = actionsLogin.buildRequest();
	} catch (FileNotFoundException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
}

@When("User send POST HTTP request with endpoint for patient Login")
public void the_send_post_http_request_with_endpoint_for_patient_login(String string) throws InvalidFormatException, IOException {
	response =actionsLogin.PatientLogin(reqSpec, currentTag);
			
}

@Then("User recieves {int} created with response body for patient Login")
public void user_recieves_created_with_response_body_for_patient_login(Integer int1) {
	statusCode =response.getStatusCode();
	expectedResponseTime=response.getTime();
	expectedContentType= response.contentType();
	API_BaseSetUp_Validations.executeAllMethods(response,statusCode,expectedResponseTime,expectedContentType);
}
@Then("User recieves {int} unauthorized for for patient Login")
public void user_recieves_unauthorized_for_for_patient_login(Integer int1) {
    
	statusCode =response.getStatusCode();
	expectedResponseTime=response.getTime();
	expectedContentType= response.contentType();
	API_BaseSetUp_Validations.executeAllMethods(response,statusCode,expectedResponseTime,expectedContentType);
}

}