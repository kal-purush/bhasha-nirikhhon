package com.api.ReusableUtils;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import java.io.File;

import io.restassured.module.jsv.JsonSchemaValidator;
import io.restassured.response.Response;

public class API_BaseSetUp_Validations {
	
	// Verify status code
    public static void validateStatusCode(Response response, int status_Code) {
        assertThat("Status code is not equal", response.getStatusCode(), is(status_Code));
    }
    // Verify status line
    public static void validateStatusLine(Response response) {
        String statusLine = response.getStatusLine();
        System.out.println("statusLine is: " + statusLine);
    }
    // Verify response time
    public static void validateResponseTime(Response response, long expecResponseTime) {
        long actualResponseTime = response.getTime();
        System.out.println("actualResponseTime is: " + actualResponseTime);
    }
    // Verify Response Schema
    public void validateResponseBodySchema(Response response, String FilePath) {
        System.out.println("Validating response schema ");
        response.then().assertThat().body(JsonSchemaValidator.matchesJsonSchema(new File(FilePath)));
    }
    // Validate content type
    public static void validateContentType(Response response, String expecContenValue) {
        String actualContentValue = response.getContentType();
        assertThat("contentType is not matching", actualContentValue, is(expecContenValue));
        System.out.println("actualContentValue is: " + actualContentValue);
    }
    // Execute all methods
    public static void executeAllMethods(Response response, int statusCode, long expectedResponseTime, String expectedContentType) {
        validateStatusCode(response, statusCode);
        validateStatusLine(response);
        validateResponseTime(response, expectedResponseTime);
        //new API_BaseSetUp().validateResponseBodySchema(response, filePath);dont FORGETTTTTto add filepath in the param
        validateContentType(response, expectedContentType);
    }
}