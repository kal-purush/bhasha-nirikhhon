package org.shikshalokam.backend;

import io.restassured.http.ContentType;
import io.restassured.response.Response;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import java.net.URI;
import java.net.URISyntaxException;
import static io.restassured.RestAssured.*;
import static org.shikshalokam.backend.PropertyLoader.PROP_LIST;
import static org.testng.Assert.*;

public class TestCRUDUserRoles extends MentorEDBaseTest {

    public static final Logger logger = LogManager.getLogger(TestCRUDUserRoles.class);
    private URI createUserRolesEndpoint, getUserRolesEndPoint, updateUserRolesEndpoint;
    private String userRoleTitle, createdRoleID,updateUserRoleTitle;

    @BeforeTest
    public void init() {
        logger.info("Logging into the application :");
        loginToMentorED(PROP_LIST.get("mentor.qa.admin.login.user").toString(), PROP_LIST.get("mentor.qa.admin.login.password").toString());

        createUserRolesEndpoint = createURI("/user/v1/user-role/create");
        getUserRolesEndPoint = createURI("/user/v1/user-role/list");

        userRoleTitle = "userroletitle" + RandomStringUtils.randomAlphabetic(5).toLowerCase();
        updateUserRoleTitle = "updusertitle" + RandomStringUtils.randomAlphabetic(3).toLowerCase();
    }

    @Test(description = "Creates user role with given title ")
    public void testCreateUserRoles() {
        logger.info("Started calling --------------- Create User Roles ----------- ");
        Response response = createUserRole(userRoleTitle, "1", "ACTIVE", "PUBLIC");

        logger.info("Response Code: {}, Response Body: {}", response.getStatusCode(), response.getBody().asString());
        assertEquals(response.getStatusCode(), 201, "User role creation failed with " + response.getStatusCode());

        getUserRolesID(true,userRoleTitle );
        assertNotNull(createdRoleID, "Role ID not found in the response");

        logger.info("Ended calling ----------------- CreateUserRoles with assertions completed");
    }

    @Test(description = "Validates the negative use case for create user role without providing title ")
    public void testCreateUserRoles_MissingRequiredFields() {
        logger.info("Started calling --------- Create User Roles - Negative Test Case Missing Required Fields");
        Response response = createUserRole("", "1", "ACTIVE", "PUBLIC");

        logger.info("Response Code: {}, Response Body: {}", response.getStatusCode(), response.getBody().asString());
        assertEquals(response.getStatusCode(), 422, "Expected Bad Request for missing required fields, but got " + response.getStatusCode());

        String responseBody = response.getBody().asString();
        assertTrue(responseBody.contains("title field is empty") || responseBody.contains("title is invalid, must not contain spaces"),
                "Expected error messages for missing title or invalid title not found in response: " + responseBody);

        logger.info("Ended calling --------------- CreateUserRoles: Negative test case completed with assertions on missing fields.");
    }

    @Test(dependsOnMethods = "testCreateUserRoles")
    public void testUpdateUserRoles() {
        logger.info("Started calling ---------------- Update User Roles ---------------");
        Response response = updateUserRole(createdRoleID, updateUserRoleTitle, "1", "ACTIVE", "PUBLIC");

        logger.info("Response Code: {}, Response Body: {}", response.getStatusCode(), response.getBody().asString());
        assertEquals(response.getStatusCode(), 201, "User role creation failed with " + response.getStatusCode());

        Response responsebody = getUserRolesID(false, updateUserRoleTitle);
        userRoleTitle = responsebody.jsonPath().getString("result.data[0].title");

        if (userRoleTitle.equals(updateUserRoleTitle))
            assertEquals(userRoleTitle,updateUserRoleTitle,"Updated role title does not match the expected value.");
        else
            assertEquals(userRoleTitle,updateUserRoleTitle,"role titles not same");
        logger.info("Ended calling ------------- UpdateUserRoles with assertions completed");
    }

    @Test(dependsOnMethods = "testCreateUserRoles")
    public void testUpdateUserRoles_MissingRequiredFields() {
        logger.info("Started calling --------- Update User Roles - Negative Test Case Missing Required Fields");
        Response response = updateUserRole(createdRoleID,"", "1", "ACTIVE", "PUBLIC");

        logger.info("Response Code: {}, Response Body: {}", response.getStatusCode(), response.getBody().asString());
        assertEquals(response.getStatusCode(), 422, "Expected Bad Request for missing required fields, but got " + response.getStatusCode());

        String responseBody = response.getBody().asString();
        assertTrue(responseBody.contains("title field is empty") || responseBody.contains("title is invalid, must not contain spaces"),
                "Expected error messages for missing title or invalid title not found in response: " + responseBody);

        logger.info("Ended calling --------------- UpdateUserRoles: Negative test case completed with assertions on missing fields.");
    }

    private URI createURI(String endpoint) {
        try {
            return new URI(endpoint);
        } catch (URISyntaxException e) {
            logger.error("Invalid URI syntax for endpoint: {}", endpoint, e);
            throw new RuntimeException("Invalid URI Syntax", e);
        }
    }

    private Response createUserRole(String title, String userType, String status, String visibility) {
        JSONObject requestBody = new JSONObject();
        requestBody.put("title", title);
        requestBody.put("user_type", userType);
        requestBody.put("status", status);
        requestBody.put("visibility", visibility);

        Response response = given()
                .header("X-auth-token", "bearer " + X_AUTH_TOKEN)
                .contentType(ContentType.JSON)
                .body(requestBody.toString())
                .when().post(createUserRolesEndpoint);
        return response;
    }

    private Response getUserRolesID(boolean idNeeded, String title) {
        logger.info("Started calling ---------- GET User Roles -------------");

        Response response = given()
                .header("X-auth-token", "bearer " + X_AUTH_TOKEN)
                .queryParam("search", title)
                .when().get(getUserRolesEndPoint);

        if (idNeeded && response!=null) {
            createdRoleID = response.jsonPath().getString("result.data[0].id"); // Extract the created user role ID from the response
            return null;
        } else {
            return response; // If idNeeded is false, return the full response
        }
    }

    public Response updateUserRole(String roleID, String title, String userType, String status, String visibility) {
        try {
            updateUserRolesEndpoint = new URI("/user/v1/user-role/update/" + roleID);
        } catch (URISyntaxException e) {
            logger.error("Invalid URI syntax for the endpoint", e);
            throw new RuntimeException("Invalid URI Syntax", e);
        }

        JSONObject requestBodyJson = new JSONObject();
        requestBodyJson.put("title", title);
        requestBodyJson.put("user_type", userType);
        requestBodyJson.put("status", status);
        requestBodyJson.put("visibility", visibility);

        logger.info(requestBodyJson.toJSONString());
        Response response = given()
                .header("X-auth-token", "bearer " + X_AUTH_TOKEN)
                .contentType(ContentType.JSON)
                .body(requestBodyJson.toString())
                .when().post(updateUserRolesEndpoint);

        return response;
    }
}
