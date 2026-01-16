package ogc.rs.restAssuredTest;

import io.restassured.RestAssured;
import io.restassured.response.Response;
import io.vertx.core.json.JsonObject;
import jdk.jfr.Description;
import ogc.rs.util.FakeTokenBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import static io.restassured.RestAssured.given;
import static ogc.rs.common.Constants.*;
import static ogc.rs.processes.tilesMetaDataOnboarding.Constants.*;
import static ogc.rs.processes.collectionOnboarding.Constants.RESOURCE_OWNERSHIP_ERROR;
import static ogc.rs.processes.util.Status.ACCEPTED;
import static ogc.rs.restAssuredTest.Constant.*;
import static org.hamcrest.Matchers.is;

@ExtendWith(RestAssuredConfigExtension.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class TilesMetaDataOnboardingProcessIT {
    private static final Logger LOGGER = LogManager.getLogger(CollectionAppendingProcessIT.class);
    String executionEndpoint = "/processes/{processId}/execution";
    String jobStatusEndpoint = "/jobs/{jobId}";
    String processId = "a512b49a-f8e6-4da1-99b0-13aa758c9104";


    @BeforeAll
    public static void setup() throws IOException {
        // Set up files for the tests
        // put the file in /fileName path
        File validFile = new File("src/test/resources/processFiles/c432b3df-0a22-485d-80a5-ace8195f6074/WebMercatorQuad/0/0/0");
        given().port(PORT).body(validFile).when().put(BUCKET_PATH + "src/test/resources/processFiles/c432b3df-0a22-485d-80a5-ace8195f6074/WebMercatorQuad/0/0/0")
                .then().statusCode(200);

        // put the file in bucket1/fileName to read file directly from S3
        given().port(PORT).body(validFile).when().put(BUCKET_PATH_FOR_S3 + "c432b3df-0a22-485d-80a5-ace8195f6074/WebMercator/0/0/0.pbf")
                .then().statusCode(200);

    }

    private JsonObject requestBody() {
        JsonObject requestBody = new JsonObject();

        JsonObject inputs = new JsonObject();
        inputs.put("resourceId", "c432b3df-0a22-485d-80a5-ace8195f6074");
        inputs.put("encoding", "MVT");
        inputs.put("tileMatrixSet", "WebMercatorQuad");
        inputs.put("testTileCoordinateIndexes", "0/0/0");
        inputs.put("description", "High-resolution satellite imagery for urban planning");
        inputs.put("title", "Urban Planning Satellite Imagery");
        inputs.put("pointOfOrigin", new Double[]{-20037508.3427892, 20037508.3427892});
        inputs.put("bbox", new Double[]{68.17751186879357,6.752782631992987,97.41289651394189,37.08834177335088});
        inputs.put("temporal", new String[]{"2023-01-01T00:00:00Z","2023-12-31T23:59:59Z"});

        requestBody.put("inputs", inputs);

        requestBody.put("response", "raw");
        return requestBody;
    }

    private String getToken() {
        return new FakeTokenBuilder().withSub(UUID.fromString("0ff3d306-9402-4430-8e18-6f95e4c03c97"))
                .withResourceServer().withRoleProvider().withCons(new JsonObject()).build();
    }

    private Response sendExecutionRequest(String processId, String token, JsonObject requestBody) {
        return RestAssured.given().pathParam("processId", processId).auth().oauth2(token)
                .contentType("application/json").body(requestBody.toString()).when().post(executionEndpoint);
    }

    private Response sendJobStatusRequest(String jobId, String token) {
        return RestAssured.given().pathParam("jobId", jobId).auth().oauth2(token)
                .get(jobStatusEndpoint);
    }

    @Test
    @Order(1)
    @Description("Failure: Unauthorized user.")
    public void testExecuteUnauthorizedFail() {
        LOGGER.debug("Testing Failure: Unauthorized user.");
        String invalidToken = new FakeTokenBuilder()
                .withSub(UUID.fromString("0ff3d306-9402-4430-8e18-6f95e4c03c97"))
                .withResourceServer().withRoleConsumer().withCons(new JsonObject()).build();
        Response response = sendExecutionRequest(processId, invalidToken, requestBody());
        response.then().statusCode(401).body(CODE_KEY, is("Not Authorized"));
    }

    @Test
    @Order(2)
    @Description("Failure: Consumer Delegate User")
    public void testExecuteWithConsumerDelegateUserFail() {
        LOGGER.debug("Testing Failure: Consumer Delegate User");
        String invalidToken = new FakeTokenBuilder()
                .withSub(UUID.fromString("0ff3d306-9402-4430-8e18-6f95e4c03c97"))
                .withResourceServer().withDelegate(UUID.fromString("9304cb99-7125-47f1-8686-a070bb6c3eaf"), "consumer")
                .withCons(new JsonObject()).build();
        Response response = sendExecutionRequest(processId, invalidToken, requestBody());
        response.then().statusCode(401).body(CODE_KEY, is("Not Authorized"));
    }

    @Test
    @Order(3)
    @Description("Failure: Provider Delegate with different provider id")
    public void testExecuteWithDifferentDelegateUserIdFail() throws InterruptedException {
        LOGGER.debug("Testing Failure: Provider Delegate with different DID");
        String invalidToken = new FakeTokenBuilder()
                .withSub(UUID.randomUUID())
                .withResourceServer().withDelegate(UUID.fromString("9304cb99-7125-47f1-8686-a070bb6c3eaf"), "provider")
                .withCons(new JsonObject()).build();
        Response sendExecutionRequest = sendExecutionRequest(processId, invalidToken, requestBody());
        String jobId = sendExecutionRequest.body().path("jobId");
        Thread.sleep(3000);
        Response getJobStatus = sendJobStatusRequest(jobId, invalidToken);
        getJobStatus.then().statusCode(200).body("message", is(RESOURCE_OWNERSHIP_ERROR));
    }

    @Test
    @Order(4)
    @Description("Success: Provider Delegate Flow")
    public void testExecuteWithProviderDelegateUser() {
        LOGGER.debug("Testing Success: Provider Delegate User");
        String token = new FakeTokenBuilder()
                .withSub(UUID.fromString("0ff3d306-9402-4430-8e18-6f95e4c03c97"))
                .withResourceServer().withDelegate(UUID.fromString("9304cb99-7125-47f1-8686-a070bb6c3eaf"), "provider")
                .withCons(new JsonObject()).build();
        Response response = sendExecutionRequest(processId, token, requestBody());
        response.then().statusCode(201).body("status", is(ACCEPTED.toString()));
    }

    @Test
    @Order(5)
    @Description("Success: Process Accepted")
    public void testExecuteProcess() {
        LOGGER.debug("Testing Success: Process Accepted");
        String token = getToken();
        Response response = sendExecutionRequest(processId, token, requestBody());
        response.then().statusCode(201).body("status", is(ACCEPTED.toString()));
    }

    @Test
    @Order(6)
    @Description("Failure: Process does not Exist")
    public void testExecuteFailProcessNotPresent() {
        LOGGER.debug("Testing Failure: Process does not Exist");
        String invalidProcessId = "a512b49a-f8e9-4da1-99b0-13aa758c9102";
        Response response = sendExecutionRequest(invalidProcessId, getToken(), requestBody());
        response.then().statusCode(404).body(TYPE_KEY, is(NOT_FOUND));
    }

    @Test
    @Order(7)
    @Description("Failure: Invalid input")
    public void testExecuteFailInvalidInput() {
        LOGGER.debug("Testing Failure: Invalid input");
        JsonObject invalidRequest = new JsonObject()
                .put("inputs", requestBody().getJsonObject("inputs").remove("encoding"));
        Response response = sendExecutionRequest(processId, getToken(), invalidRequest);
        response.then().statusCode(400).body(CODE_KEY, is("Bad Request"));
    }

    @Test
    @Order(8)
    @Description("Failure: Ownership Error")
    public void testExecuteFailOwnershipError() throws InterruptedException {
        LOGGER.debug("Failure: Ownership Error");

        String token = new FakeTokenBuilder().withSub(UUID.randomUUID())
                .withResourceServer().withRoleProvider().withCons(new JsonObject()).build();
        Response sendExecutionRequest = sendExecutionRequest(processId, token, requestBody());
        String jobId = sendExecutionRequest.body().path("jobId");
        Thread.sleep(3000);
        Response getJobStatus = sendJobStatusRequest(jobId, token);
        getJobStatus.then().statusCode(200).body("message", is(RESOURCE_OWNERSHIP_ERROR));
    }

    @Test
    @Order(9)
    @Description("Failure: Check the encoding format")
    public void testFailCheckEncodingFormat() throws InterruptedException {
        LOGGER.debug("Failure: Invalid Encoding Format");

        String token = getToken();
        JsonObject requestBody = requestBody();
        requestBody.getJsonObject("inputs").put("title", "Non-existing encoding format test")
                .put("encoding", "SVG");
        Response sendExecutionRequest = sendExecutionRequest(processId, token, requestBody);
        String jobId = sendExecutionRequest.body().path("jobId");
        Thread.sleep(3000);
        Response getJobStatus = sendJobStatusRequest(jobId, token);
        getJobStatus.then().statusCode(200).body("message", is(INVALID_ENCODING_FORMAT_MESSAGE));
    }

    @Test
    @Order(10)
    @Description("Failure: Check the tile matrix set")
    public void testFailCheckTileMatrixSet() throws InterruptedException {
        LOGGER.debug("Failure: Invalid TileMatrixSet");

        String token = getToken();
        JsonObject requestBody = requestBody();
        requestBody.getJsonObject("inputs").put("title", "Non-existing tile matrix set test")
                .put("tileMatrixSet", "EuropeanETRS89_LAEAQuad");
        Response sendExecutionRequest = sendExecutionRequest(processId, token, requestBody);
        String jobId = sendExecutionRequest.body().path("jobId");
        Thread.sleep(3000);
        Response getJobStatus = sendJobStatusRequest(jobId, token);
        getJobStatus.then().statusCode(200).body("message", is(TILE_MATRIX_SET_NOT_FOUND_MESSAGE));
    }

    @Test
    @Order(11)
    @Description("Failure: Check the file existence in S3")
    public void testFailCheckFileExistenceInS3() throws InterruptedException {
        LOGGER.debug("Failure: Invalid File which does not exist in S3");

        String token = getToken();
        JsonObject requestBody = requestBody();
        requestBody.getJsonObject("inputs").put("title", "Non-existing file in S3 test")
                .put("tileCoordinateIndexes", "0/0/1");
        Response sendExecutionRequest = sendExecutionRequest(processId, token, requestBody);
        String jobId = sendExecutionRequest.body().path("jobId");
        Thread.sleep(3000);
        Response getJobStatus = sendJobStatusRequest(jobId, token);
        getJobStatus.then().statusCode(200).body("message", is(S3_FILE_EXISTENCE_FAIL_MESSAGE));
    }

    @Test
    @Order(12)
    @Description("Success: Onboarding Tiles Meta Data")
    public void testExecuteTilesMetaDataOnboardingSuccess() throws InterruptedException {
        LOGGER.debug("Success: Onboarding Tiles Meta Data");

        String token = getToken();
        JsonObject requestBody = requestBody();
        requestBody.getJsonObject("inputs").put("title", "Tiles Meta Data Onboarding Process Success")
                .put("description", "Testing Tiles Meta Data Onboarding Process Success");
        Response sendExecutionRequest = sendExecutionRequest(processId, token, requestBody);
        String jobId = sendExecutionRequest.body().path("jobId");
        Thread.sleep(40000);
        Response getJobStatus = sendJobStatusRequest(jobId, token);
        getJobStatus.then().statusCode(200).body("message", is(TILES_METADATA_ONBOARDING_SUCCESS_MESSAGE));
    }
    @Test
    @Order(13)
    @Description("Failure: Onboarding Tiles Meta Data as collection is already present")
    public void testExecuteTileCollectionAlreadyPresent() throws InterruptedException {
        LOGGER.debug("Failure: Onboarding Tiles Meta Data");

        String token = getToken();
        JsonObject requestBody = requestBody();
        Response sendExecutionRequest = sendExecutionRequest(processId, token, requestBody);
        String jobId = sendExecutionRequest.body().path("jobId");
        Thread.sleep(40000);
        Response getJobStatus = sendJobStatusRequest(jobId, token);
        getJobStatus.then().statusCode(200).body("message", is(COLLECTION_EXISTS_MESSAGE));
    }

}