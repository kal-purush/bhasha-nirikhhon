package org.shikshalokam.backend.ep;

import io.restassured.http.ContentType;
import io.restassured.response.Response;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.shikshalokam.backend.PropertyLoader;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;

import static io.restassured.RestAssured.*;
import static org.shikshalokam.backend.PropertyLoader.PROP_LIST;
import static org.testng.Assert.*;

public class TestCRUDuserRoleExtentionOperations extends ElevateProjectBaseTest {

    public static final Logger logger = LogManager.getLogger(TestCRUDuserRoleExtentionOperations.class);
    private URI createUserRoleExtensionEndpoint, updateUserRoleExtensionEndpoint, findUserRoleExtensionEndpoint, deleteUserRoleExtensionEndpoint;
    private String createdRoleID, userRoleTitle, updatedRoleTitle = "Principal";

    @BeforeTest
    public void init() throws URISyntaxException {
        logger.info("Logging into the application :");
        loginToElevate
                (PROP_LIST.get("elevate.login.contentcreator").toString(), PROP_LIST.get("elevate.login.contentcreator.password").toString());
        createUserRoleExtensionEndpoint = createURI("/entity-management/v1/userRoleExtension/create");
        updateUserRoleExtensionEndpoint = createURI("/entity-management/v1/userRoleExtension/update");
        findUserRoleExtensionEndpoint = createURI("/entity-management/v1/userRoleExtension/find");
        deleteUserRoleExtensionEndpoint = createURI("/entity-management/v1/userRoleExtension/delete");

        userRoleTitle = "userRoleExt" + RandomStringUtils.randomAlphabetic(5).toLowerCase();
    }

    @Test(description = "Creates user role extension with given parameters")
    public void testCreateUserRoleExtension() {
        logger.info("Starting --------------- Create User Role Extension -----------");

        Response response = createUserRoleExtension(userRoleTitle, "rrddrr", "block", "6673131ec05aa58f89ba12fa");

        logger.info("Response Code: {}, Response Body: {}", response.getStatusCode(), response.getBody().asString());
        assertEquals(response.getStatusCode(), 200, "User role extension creation failed with " + response.getStatusCode());

        createdRoleID = response.jsonPath().getString("result._id");
        assertNotNull(createdRoleID, "Role ID not found in the response");

        logger.info("Ended CreateUserRoleExtension with assertions completed");
    }

    @Test(dependsOnMethods = "testCreateUserRoleExtension", description = "Updates the user role extension")
    public void testUpdateUserRoleExtension() {
        logger.info("Starting ---------------- Update User Role Extension ---------------");

        // Use the createdRoleID directly for the update
        Response response = updateUserRoleExtension(createdRoleID, updatedRoleTitle, "block", "6673131ec05aa58f89ba12fa");

        logger.info("Response Code: {}, Response Body: {}", response.getStatusCode(), response.getBody().asString());
        assertEquals(response.getStatusCode(), 200, "User role extension update failed with " + response.getStatusCode());

        logger.info("Ended UpdateUserRoleExtension with assertions completed");
    }

    @Test(dependsOnMethods = "testUpdateUserRoleExtension", description = "Finds the user role extension by status")
    public void testFindUserRoleExtension() {
        logger.info("Starting ---------------- Find User Role Extension ---------------");

        Response response = findUserRoleExtension("ACTIVE");

        logger.info("Response Code: {}, Response Body: {}", response.getStatusCode(), response.getBody().asString());
        assertEquals(response.getStatusCode(), 200, "User role extension find request failed with " + response.getStatusCode());

        assertTrue(response.getBody().asString().contains(updatedRoleTitle), "Expected role title 'Principal' not found in the response.");
        logger.info("Ended FindUserRoleExtension with assertions completed");
    }

    @Test(dependsOnMethods = "testUpdateUserRoleExtension", description = "Deletes the user role extension")
    public void testDeleteUserRoleExtension() {
        logger.info("Starting ------------- Delete User Role Extension ------------");

        // Use the createdRoleID directly for the delete
        Response response = deleteUserRoleExtension(createdRoleID);

        logger.info("Response Code: {}, Response Body: {}", response.getStatusCode(), response.getBody().asString());
        assertEquals(response.getStatusCode(), 200, "User role extension deletion failed with " + response.getStatusCode());

        logger.info("Ended DeleteUserRoleExtension with assertions completed");
    }

    private URI createURI(String endpoint) {
        try {
            return new URI(endpoint);
        } catch (URISyntaxException e) {
            logger.error("Invalid URI syntax for endpoint: {}", endpoint, e);
            throw new RuntimeException("Invalid URI Syntax", e);
        }
    }

    private Response createUserRoleExtension(String title, String userRoleId, String entityType, String entityTypeId) {
        HashMap<String, Object> requestBody = new HashMap<>();
        requestBody.put("title", title);
        requestBody.put("userRoleId", userRoleId);
        requestBody.put("code", userRoleId);
        requestBody.put("entityTypes", List.of(new HashMap<String, String>() {{
            put("entityType", entityType);
            put("entityTypeId", entityTypeId);
        }}));

        return given()
                .header("X-auth-token", X_AUTH_TOKEN)
                .header("internal-access-token", INTERNAL_ACCESS_TOKEN)
                .contentType(ContentType.JSON)
                .body(requestBody)
                .when().post(createUserRoleExtensionEndpoint);
    }

    private Response updateUserRoleExtension(String roleID, String title, String entityType, String entityTypeId) {
        HashMap<String, Object> requestBody = new HashMap<>();
        requestBody.put("title", title);
        requestBody.put("entityTypes", List.of(new HashMap<String, String>() {{
            put("entityType", entityType);
            put("entityTypeId", entityTypeId);
        }}));

        // Update the URL to include the role ID
        String updateUrlWithRoleId = updateUserRoleExtensionEndpoint.toString() + "/" + roleID;

        return given()
                .header("X-auth-token", X_AUTH_TOKEN)
                .header("internal-access-token", INTERNAL_ACCESS_TOKEN)
                .contentType(ContentType.JSON)
                .body(requestBody)
                .when().post(updateUrlWithRoleId); // Use the updated URL here
    }

    private Response deleteUserRoleExtension(String roleID) {
        // Update the URL to include the role ID
        String deleteUrlWithRoleId = deleteUserRoleExtensionEndpoint.toString() + "/" + roleID;

        return given()
                .header("X-auth-token", X_AUTH_TOKEN)
                .header("internal-access-token", INTERNAL_ACCESS_TOKEN)
                .contentType(ContentType.JSON)
                .when().delete(deleteUrlWithRoleId); // Use the updated URL here
    }

    private String findUserRoleID(String status, String title) {
        logger.info("Starting ---------------- Find User Role ID ---------------");

        Response response = findUserRoleExtension(status);
        assertEquals(response.getStatusCode(), 200, "Find request failed with status code: " + response.getStatusCode());

        String foundRoleID = response.jsonPath().getString("result.find { it.title == '" + title + "' }._id");
        assertNotNull(foundRoleID, "Role ID for title '" + title + "' not found in the response.");

        logger.info("Found Role ID: {}", foundRoleID);
        return foundRoleID;
    }

    private Response findUserRoleExtension(String status) {
        HashMap<String, Object> requestBody = new HashMap<>();
        HashMap<String, String> query = new HashMap<>();
        query.put("status", status);
        requestBody.put("query", query);
        requestBody.put("projection", List.of("all"));

        return given()
                .header("X-auth-token", X_AUTH_TOKEN)
                .header("internal-access-token", INTERNAL_ACCESS_TOKEN)
                .contentType(ContentType.JSON)
                .body(requestBody)
                .when().post(findUserRoleExtensionEndpoint);
    }
}