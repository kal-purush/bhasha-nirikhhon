package org.shikshalokam.frontend.scp;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import io.restassured.response.Response;
import static io.restassured.RestAssured.given;
import org.shikshalokam.backend.PropertyLoader;
import org.shikshalokam.backend.scp.SelfCreationPortalBaseTest;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestScpGetPermissionsAPI extends SelfCreationPortalBaseTest {
    static final Logger logger = LogManager.getLogger(TestScpGetPermissionsAPI.class);
    public static String X_AUTH_TOKEN = null;
    public static Response response = null;

    @BeforeClass
    public void setUp() {
        // Logging in to get the X-AUTH-TOKEN
        response = SelfCreationPortalBaseTest.loginToScp(PropertyLoader.PROP_LIST.getProperty("sl.scp.userascontentcreator"), PropertyLoader.PROP_LIST.getProperty("sl.scp.passwordforcontentcreator"));
        Assert.assertEquals(response.getStatusCode(), 200, "Login failed!");

        X_AUTH_TOKEN = response.jsonPath().getString("token");
        logger.info("X-AUTH-TOKEN retrieved successfully: " + X_AUTH_TOKEN);
    }

    // Test for getting permissions
    @Test(description = "Verifies the getPermissions API for a valid user.")
    public void testGetPermissionsApi() {
        String apiUrl = "https://qa.elevate-apis.shikshalokam.org/scp/v1/permissions/getPermissions";

        // Sending a GET request to the getPermissions API
        response = given()
                .header("Authorization", "Bearer " + X_AUTH_TOKEN)  // Passing the authentication token
                .when()
                .get(apiUrl)
                .then()
                .extract().response();

        // Validate the response status code
        Assert.assertEquals(response.getStatusCode(), 200, "Failed to fetch permissions.");

        // Check that the permissions list is not empty
        Assert.assertNotNull(response.jsonPath().getList("permissions"), "Permissions list should not be empty.");

        logger.info("Permissions retrieved successfully: " + response.getBody().asString());
    }

    // Test for invalid token case
    @Test(description = "Verifies the getPermissions API with an invalid token.")
    public void testGetPermissionsApiWithInvalidToken() {
        String apiUrl = "https://qa.elevate-apis.shikshalokam.org/scp/v1/permissions/getPermissions";

        // Sending a GET request with an invalid token
        response = given()
                .header("Authorization", "Bearer invalidToken")  // Passing an invalid token
                .when()
                .get(apiUrl)
                .then()
                .extract().response();

        // Validate the response status code
        Assert.assertEquals(response.getStatusCode(), 401, "Expected a 401 Unauthorized error for invalid token.");

        logger.info("Received expected error for invalid token: " + response.getBody().asString());
    }
}

