package uk.gov.hmcts.reform.amapi.functional;

import com.fasterxml.jackson.core.JsonPointer;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.restassured.path.json.JsonPath;
import io.restassured.response.Response;
import lombok.extern.slf4j.Slf4j;
import net.serenitybdd.junit.spring.integration.SpringIntegrationSerenityRunner;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ActiveProfiles;
import uk.gov.hmcts.reform.amlib.enums.Permission;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static uk.gov.hmcts.reform.amlib.enums.AccessorType.USER;
import static uk.gov.hmcts.reform.amlib.enums.Permission.CREATE;
import static uk.gov.hmcts.reform.amlib.enums.Permission.READ;
import static uk.gov.hmcts.reform.amlib.enums.Permission.UPDATE;

@Slf4j
@RunWith(SpringIntegrationSerenityRunner.class)
@ActiveProfiles("functional")
@SuppressWarnings({"PMD.JUnitTestsShouldIncludeAssert","PMD.AvoidDuplicateLiterals","PMD.Incremental Analysis"})
public class AccessManangementApiTest extends FunctionalTestSuite {

    Response response;

    String resourceId = "Siva1234";
    @NotEmpty Set<@NotBlank String> accessorIds;
    String accessorId = "Test1234";
    String resourceName = "claim-test";
    String resourceType = "case-test";
    String seriviceName = "cmc-test";
    String relationship = "caseworker-test";
    Map<JsonPointer, Set<Permission>> multipleAttributePermissions = ImmutableMap.of(
        JsonPointer.valueOf(""), ImmutableSet.of(CREATE, READ, UPDATE));

    @Test
    public void verifyGrantExplicitAccessApi() {
        String expectedResourceDef = "{resourceName=" + amApiClient.getResourceName()
            + ", serviceName=" + amApiClient.getSeriviceName()
            + ", resourceType=" + amApiClient.getResourceType()
            + "}";
        try {
            Response response = amApiClient.createExplicitAccess().post(amApiClient.getAccessUrl()
                + "api/access-resource");
            response.then().statusCode(201);
            response.then().log();
            JsonPath responseBody = response.getBody().jsonPath();
            assertThat(responseBody.get("accessorIds").toString()).isEqualTo("[" + amApiClient.getAccessorId() + "]");
            assertThat(responseBody.get("resourceId").toString()).isEqualTo(amApiClient.getResourceId());
            assertThat(responseBody.get("resourceDefinition").toString()).isEqualTo(expectedResourceDef);
            assertThat(responseBody.get("relationship").toString()).isEqualTo(amApiClient.getRelationship());
            assertThat(responseBody.get("attributePermissions").toString()).contains("CREATE");
            assertThat(responseBody.get("attributePermissions").toString()).contains("READ");
            assertThat(responseBody.get("attributePermissions").toString()).contains("UPDATE");
            assertThat(responseBody.get("accessorType").toString()).isEqualTo(USER.toString());
            response.then().log();
        } catch (Exception e) {
            log.error("verifyGrantExplicitAccessApi : " + e.toString());
        }
    }

    @Test
    public void verifyFilterResourceApi() {
        Response accessResponse = amApiClient.createExplicitAccess().post(amApiClient.getAccessUrl()
            + "api/access-resource");
        JsonPath responseBody =  accessResponse.getBody().jsonPath();
        Response response = amApiClient.createFilterAccess(responseBody.get("resourceId").toString(),
            amApiClient.getAccessorId()).post(amApiClient.getAccessUrl()
            + "api/filter-resource");
        response.then().statusCode(200);
        response.then().log();
    }

    @Test
    public void verifyRevokeExplicitAccessApi() {
        Response accessResponse = amApiClient.createExplicitAccess().post(amApiClient.getAccessUrl()
            + "api/access-resource");
        JsonPath responseBody =  accessResponse.getBody().jsonPath();
        Response response = amApiClient.createRevokeAccess(amApiClient.getAccessorId(),
            responseBody.get("resourceId").toString()).delete(amApiClient.getAccessUrl()
            + "api/access-resource");
        response.then().statusCode(204);
        response.then().log();
    }

    @Test
    public void verifyRevokeExplicitAccessApiWithoutResourcenameAndServicename() {
        Response accessResponse = amApiClient.createExplicitAccess().post(amApiClient.getAccessUrl()
            + "api/access-resource");
        JsonPath responseBody =  accessResponse.getBody().jsonPath();
        Response response = amApiClient.createRevokeAccessWithoutOptionalParams(amApiClient.getAccessorId(),
            responseBody.get("resourceId").toString()).delete(amApiClient.getAccessUrl()
            + "api/access-resource");
        response.then().statusCode(204);
        response.then().log();
    }

    @Test
    public void verifyGrantExplicitAccessErrorScenariosInvalidMediaType() {
        try {
            Response response = amApiClient.createExplicitAccess()
                .header("Content-Type", "application/xml")
                .post(amApiClient.getAccessUrl() + "api/access-resource");
            response.then().statusCode(415);
        } catch (Exception e) {
            log.error("verifyGrantExplicitAccessApi : " + e.toString());
            response.then().log();
        }
    }

    @Test
    public void verifyGrantExplicitAccessErrorScenariosWrongEndpoint() {
        Response response = amApiClient.createExplicitAccess().post(amApiClient.getAccessUrl()
            + "api/access-resource-test");
        response.then().statusCode(404);
    }

}