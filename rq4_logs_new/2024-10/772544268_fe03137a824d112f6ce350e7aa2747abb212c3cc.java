package org.shikshalokam.backend;

import io.restassured.response.Response;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.OptionalDataException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static io.restassured.RestAssured.given;
import static org.shikshalokam.backend.PropertyLoader.PROP_LIST;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestGetListofSessions extends MentorEDBaseTest {
    private static final Logger logger = LogManager.getLogger(TestGetListofSessions.class);
    private URI getSessionList;

    @BeforeMethod
    public void init() {
        logger.info("Logging into the application :");
        loginToMentorED(PROP_LIST.get("mentor.qa.admin.login.user").toString(),
                PROP_LIST.get("mentor.qa.admin.login.password").toString());

        getSessionList = MentorBase.createURI("mentoring/v1/mentees/homeFeed");
    }

    @Test
    public void testGetListofSessions() {
        logger.info("Started calling the get list of sessions");

        Response response = given()
                .header("X-auth-token", "bearer " + X_AUTH_TOKEN)
                .get(getSessionList);
        //response.prettyPrint();


        assertEquals(response.getStatusCode(), 200, "Status code failed for fetching list of sessions");
        List<Object> sessionList = response.jsonPath().getList("result.all_sessions");
        assertTrue(sessionList.size() > 0, "Expected count to be greater than zero");
        logger.info("Get Sessions list complete with assertions");
    }
}





