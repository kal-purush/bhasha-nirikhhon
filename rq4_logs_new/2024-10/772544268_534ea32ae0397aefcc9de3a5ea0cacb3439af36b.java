package org.shikshalokam.backend.ep;

import io.restassured.http.ContentType;
import io.restassured.response.Response;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.shikshalokam.backend.PropertyLoader;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static io.restassured.RestAssured.given;
import static org.testng.Assert.*;

public class TestCRUDUserRoleExtension extends ElevateProjectBaseTest {

    public static final Logger logger = LogManager.getLogger(TestCRUDUserRoleExtension.class);
    private URI createUserRoleExtensionEndpoint;
    private String userRoleId, title, entityType, entityTypeId, code;

    @BeforeTest
    public void init() {
        logger.info("Logging into the application...");
        loginToElevate(
                PropertyLoader.PROP_LIST.getProperty("elevate.login.contentcreator"),
                PropertyLoader.PROP_LIST.getProperty("elevate.login.contentcreator.password")
        );

        try {
            createUserRoleExtensionEndpoint = new URI(PropertyLoader.PROP_LIST.getProperty("userRoleExtension.create.endpoint"));
        } catch (URISyntaxException e) {
            logger.error("Invalid URI syntax for endpoint", e);
            throw new RuntimeException("Invalid URI Syntax", e);
        }

        // Set default test data
        userRoleId = "userextension" + RandomStringUtils.randomAlphabetic(10).toLowerCase();
        title = "userTitle" + RandomStringUtils.randomAlphabetic(10).toLowerCase();
        code = "code" + userRoleId;
    }

    @Test(description = "Creates user role extension with valid data")
    public void testCreateUserRoleExtension() {
        logger.info("Testing creation of User Role Extension with valid details...");

        // Create HashMap for the request body
        HashMap<String, Object> requestBody = new HashMap<>();
        requestBody.put("userRoleId", "createID" + RandomStringUtils.randomAlphabetic(8).toLowerCase());
        requestBody.put("title", "create title" + RandomStringUtils.randomAlphabetic(8).toLowerCase());
        requestBody.put("code", "eeeee");

        // Create list of entity types
        List<HashMap<String, String>> entityTypesList = new ArrayList<>();
        HashMap<String, String> entityTypeObj = new HashMap<>();
        entityTypeObj.put("entityType", "block");
        entityTypeObj.put("entityTypeId", "6673131ec05aa58f89ba12fa");

        // Add the entity type to the list
        entityTypesList.add(entityTypeObj);

        // Add entityTypes list to the request body
        requestBody.put("entityTypes", entityTypesList);

        // Log the request body
        logger.info("Request Body: " + requestBody);

        // Call the createPermission method
        Response response = createUserRoleExtension(requestBody);

        // Positive Test Case - Valid Data
        logger.info("Response Code: {}, Response Body: {}", response.getStatusCode(), response.getBody().asString());
        assertEquals(response.getStatusCode(), 200, " creation failed with " + response.getStatusCode());

        logger.info("Positive test case passed: User Role Extension created successfully");
    }


    private Response createUserRoleExtension(HashMap<String, Object> createMap) {
        Response response = given()
                .header("X-auth-token", X_AUTH_TOKEN)
                .header("internal-access-token", INTERNAL_ACCESS_TOKEN)
                .contentType(ContentType.JSON)
                .body(createMap)
                .when().post(createUserRoleExtensionEndpoint);
        return response;
    }

}