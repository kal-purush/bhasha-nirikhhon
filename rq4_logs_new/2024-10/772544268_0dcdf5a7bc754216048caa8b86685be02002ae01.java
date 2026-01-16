package org.shikshalokam.backend;

import io.restassured.response.Response;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.net.URI;
import java.net.URISyntaxException;

import static io.restassured.RestAssured.given;
import static org.shikshalokam.backend.PropertyLoader.PROP_LIST;
import static org.testng.Assert.assertEquals;

public class TestMenteeReport extends MentorEDBaseTest {
    private static final Logger logger = LogManager.getLogger(TestMenteeReport.class);

    @BeforeTest
    public void init() {
        logger.info("Logging into the application :");
        loginToMentorED(PROP_LIST.get("mentor.qa.admin.login.user").toString(),
                PROP_LIST.get("mentor.qa.admin.login.password").toString());
    }

    @Test
    public void testMenteeReport() {

        logger.info("Started calling the get mentee reports:");
        URI endpoint = null;
        try {
            endpoint = new URI("mentoring/v1/mentees/reports?filterType=QUARTERLY");
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        Response responce = given()
                .header("X-auth-token", "bearer " + X_AUTH_TOKEN)
                .get(endpoint);
        logger.info("Response Body:\n" + responce.asPrettyString());
        assertEquals(responce.getStatusCode(), 200, "Status code failed for fetching Mentee report");
        int totalSessionsEnrolled = responce.getBody().jsonPath().getInt("result.total_session_enrolled");
        System.out.println(responce.getBody().jsonPath().getInt("result.total_session_attended"));
        int totalSessionsAttended = responce.getBody().jsonPath().getInt("result.total_session_attended");
        assertEquals(totalSessionsEnrolled, 2, "Total sessions enrolled not valid");
        assertEquals(totalSessionsAttended, 0, "Total sessions attended not valid");
//        assertEquals(totalSessionsEnrolled, 2, "Total sessions enrolled not valid");
//        assertEquals(totalSessionsAttended, 1, "Total sessions attended not valid");
        logger.info("Ended calling the get Mentee Reports: with all the assertions");
    }

}





