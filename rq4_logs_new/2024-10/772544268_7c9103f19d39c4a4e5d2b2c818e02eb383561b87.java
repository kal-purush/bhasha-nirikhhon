package org.shikshalokam.backend.scp;

import io.restassured.RestAssured;
import io.restassured.response.Response;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.shikshalokam.backend.PropertyLoader;
import org.shikshalokam.frontend.scp.TestScpGetPermissionsAPI;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.net.URI;
import java.net.URISyntaxException;

import static io.restassured.RestAssured.given;

public class TestScpGetListPermission extends SelfCreationPortalBaseTest{
    static final Logger logger = LogManager.getLogger(TestScpGetPermissionsAPI.class);
    public static String X_AUTH_TOKEN = null;
    public static Response response = null;

    @BeforeTest
    public void setUp() {
        // Logging in to get the X-AUTH-TOKEN
        response = SelfCreationPortalBaseTest.loginToScp(PropertyLoader.PROP_LIST.getProperty("scp.qa.admin.login.user"), PropertyLoader.PROP_LIST.getProperty("scp.qa.admin.login.password"));
        Assert.assertEquals(response.getStatusCode(), 200, "Login failed!");

       // X_AUTH_TOKEN = response.jsonPath().getString("token");
        logger.info("X-AUTH-TOKEN retrieved successfully: " + X_AUTH_TOKEN);
    }

    // Test for getting permissions
    @Test(description = "Verifies the getPermissions API for a valid user.")
    public void testGetPermissionsApi() throws URISyntaxException {

        RestAssured.baseURI = PropertyLoader.PROP_LIST.get("scp.qa.api.base.url").toString();
        // Sending a GET request to the getPermissions API

        response = given()
                .header("Authorization", "Bearer " + X_AUTH_TOKEN)  // Passing the authentication token
                .when()
                .get(new URI(PropertyLoader.PROP_LIST.get("scp.qa.permission.list").toString()))
                .then()
                .extract().response();

        // Validate the response status code
        Assert.assertEquals(response.getStatusCode(), 200, "Failed to fetch permissions.");

        // Check that the permissions list is not empty
//        Assert.assertNotNull(response.jsonPath().getList("permissions"), "Permissions list should not be empty.");

        logger.info("Permissions retrieved successfully: " + response.getBody().asString());
    }

    // Test for invalid token case
    @Test(description = "Verifies the getPermissions API with an invalid token.")
    public void testGetPermissionsApiWithInvalidToken() throws URISyntaxException {

        RestAssured.baseURI = PropertyLoader.PROP_LIST.get("scp.qa.api.base.url").toString();
        // Sending a GET request with an invalid token
        X_AUTH_TOKEN = null;
        Response responseInvalidToken = given()
                .header("X-auth-token", "bearer token")
                .log().all() // Passing an invalid token
                .when()
                .get(new URI(PropertyLoader.PROP_LIST.get("scp.qa.permission.list").toString()))
                .then()

                .extract().response();

        responseInvalidToken.prettyPrint();

        System.out.println("Response Status Code: " + responseInvalidToken.getStatusCode());
        System.out.println("Response Body: " + responseInvalidToken.getBody().asString());

        // Validate the response status code
        Assert.assertEquals(responseInvalidToken.getStatusCode(), 401, "Expected a 401 Unauthorized error for invalid token.");

        logger.info("Received expected error for invalid token: " + responseInvalidToken.getBody().asString());
    }
}