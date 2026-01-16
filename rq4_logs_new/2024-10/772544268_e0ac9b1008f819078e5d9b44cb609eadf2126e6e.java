package org.shikshalokam.backend;

import io.restassured.http.ContentType;
import io.restassured.response.Response;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import static io.restassured.RestAssured.given;
import static org.shikshalokam.backend.PropertyLoader.PROP_LIST;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestCRUDUserPermissions extends MentorEDBaseTest {

    public static final Logger logger = LogManager.getLogger(org.shikshalokam.backend.TestCRUDUserPermissions.class);
    private URI createUserPermissionEndpoint, getUserPermissionsEndPoint;
    private String createdPermissionID,permissionName;

    @BeforeTest
    public void init() {
        logger.info("Logging into the application :");
        loginToMentorED(PROP_LIST.get("mentor.qa.sysadmin.login.user").toString(), PROP_LIST.get("mentor.qa.sysadmin.login.password").toString());

        createUserPermissionEndpoint = createURI("/user/v1/permissions/create");
        getUserPermissionsEndPoint = createURI("/user/v1/permissions/list");

        // Generate a random permission name
        permissionName = "permname" + RandomStringUtils.randomAlphabetic(10).toLowerCase();
    }

    @Test(description = "Validates the creation of permissions")
    public void testCreatePermission() {
        logger.info("Started calling ---- CreatePermission API");

        HashMap<String, Object> permissionMap = new HashMap<>();
        permissionMap.put("code", permissionName);
        permissionMap.put("module", "manage_user");

        // Example request type as a List (JSONObject handles this automatically)
        List<String> requestTypeList = new ArrayList<>();
        requestTypeList.add("POST");
        permissionMap.put("request_type", requestTypeList);

        permissionMap.put("api_path", "/mentoring/v1/entity/create");
        permissionMap.put("status", "ACTIVE");

        // Call the createPermission method
        Response response = createPermission(permissionMap);
        logger.info("Response Code: {}, Response Body: {}", response.getStatusCode(), response.getBody().asString());

        if (response.getStatusCode() == 422 || response.getStatusCode() == 400) {
            logger.error("Validation Error: " + response.getBody().asPrettyString());
            Assert.fail("Permission creation failed due to validation errors.");
        } else {
            assertEquals(response.getStatusCode(), 201, "Permission creation failed with" + response.getStatusCode());

            createdPermissionID = String.valueOf(response.jsonPath().getInt("result.Id"));
            Assert.assertNotNull(createdPermissionID, "Created Permission ID should not be null");
            logger.info("Created Permission ID: " + createdPermissionID);
        }
        logger.info("Ended calling ---- CreatePermission API with assertions completed.");
    }

    @Test(description = "Validates the negative use case for CREATE PERMISSION ")
    public void testCreatePermission_MissingRequiredFields() {
        logger.info("Started calling --------- CreatePermission - Negative Test Missing Required Fields");

        HashMap<String, Object> permissionMap = new HashMap<>();
        permissionMap.put("code", "");
        permissionMap.put("module", "");

        // Example request type as a List (JSONObject handles this automatically)
        List<String> requestTypeList = new ArrayList<>();
        requestTypeList.add("POST");
        permissionMap.put("request_type", requestTypeList);

        permissionMap.put("api_path", "/mentoring/v1/entity/create");
        permissionMap.put("status", "ACTIVE");

        // Call the createPermission method
        Response response = createPermission(permissionMap);
        logger.info("Response Code: {}, \n Response Body: {}", response.getStatusCode(), response.getBody().asString());
        assertEquals(response.getStatusCode(), 422, "Expected Bad Request for missing required fields - 422, but got " + response.getStatusCode());

        String responseBody = response.getBody().asString();
        assertTrue(responseBody.contains("Code field is empty") || responseBody.contains("Code is invalid, must not contain spaces")
                || responseBody.contains("Module field is empty") || responseBody.contains("Module is invalid, must not contain spaces")
                || responseBody.contains("request_type field is empty") || responseBody.contains("request_type is invalid, must be one of: GET,POST,PATCH,PUT,DELETE")
                || responseBody.contains("API Path is invalid") || responseBody.contains("Status is invalid, must not contain spaces"),
                "Expected error messages for missing title or invalid title not found in response: " + responseBody);

        logger.info("Ended calling --------------- CreateUserRoles: Negative test completed with assertions on missing fields.");
    }
    @Test(description = "Fetches the list of available permissions already created")
    public void getUserPermissions() {
        logger.info("Started calling ---------- GET Permissions -------------");

        Response response = given()
                .header("X-auth-token", "bearer " + X_AUTH_TOKEN)
                .when().get(getUserPermissionsEndPoint);

        logger.info(response.getBody().asPrettyString());
        assertEquals(response.getStatusCode(), 200, "Status code failed for getting permissions");

        int permissionCount = response.jsonPath().getInt("result.results.count");
        assertTrue(permissionCount > 0, "Expected permission count to be greater than zero");
        logger.info("Total permissions fetched: " + permissionCount);

        logger.info("Ended calling ---- GET Permissions: with all the assertions");
    }

    private URI createURI(String endpoint) {
        try {
            return new URI(endpoint);
        } catch (URISyntaxException e) {
            logger.error("Invalid URI syntax for endpoint: {}", endpoint, e);
            throw new RuntimeException("Invalid URI Syntax", e);
        }
    }

    public Response createPermission(HashMap<String, Object> permissionMap) {
        JSONObject jsonObject = new JSONObject();
        jsonObject.putAll(permissionMap);

        Response response = given()
                .header("X-auth-token", "bearer " + X_AUTH_TOKEN)
                .contentType(ContentType.JSON)
                .body(jsonObject)
                .when().post(createUserPermissionEndpoint);

        response.prettyPrint();
        return response;
    }
}

