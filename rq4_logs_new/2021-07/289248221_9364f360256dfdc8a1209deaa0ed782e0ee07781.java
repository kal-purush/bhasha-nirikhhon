package uk.gov.hmcts.reform.lrdapi;

import com.fasterxml.jackson.core.JsonProcessingException;
import net.serenitybdd.junit.spring.integration.SpringIntegrationSerenityRunner;
import net.thucydides.core.annotations.WithTag;
import net.thucydides.core.annotations.WithTags;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.http.HttpStatus;
import uk.gov.hmcts.reform.lrdapi.controllers.advice.ErrorResponse;
import uk.gov.hmcts.reform.lrdapi.controllers.response.LrdCourtTypeResponse;
import uk.gov.hmcts.reform.lrdapi.controllers.response.LrdCourtVenueResponse;
import uk.gov.hmcts.reform.lrdapi.controllers.response.LrdCourtVenuesByServiceCodeResponse;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static uk.gov.hmcts.reform.lrdapi.util.FeatureConditionEvaluation.FORBIDDEN_EXCEPTION_LD;

@RunWith(SpringIntegrationSerenityRunner.class)
@WithTags({@WithTag("testType:Integration")})
@SuppressWarnings("unchecked")
public class RetrieveCourtVenuesByServiceCodeIntegrationTest extends LrdAuthorizationEnabledIntegrationTest {

    public static final String HTTP_STATUS = "http_status";

    @Test
    public void returnsCourtVenuesByServiceCodeWithStatusCode_200() throws JsonProcessingException {

        LrdCourtVenuesByServiceCodeResponse response = (LrdCourtVenuesByServiceCodeResponse)
            lrdApiClient.findCourtVenuesByServiceCode("BFA1", LrdCourtVenuesByServiceCodeResponse.class);

        assertThat(response).isNotNull();
        responseVerification(response);
    }

    @Test
    public void returnsCourtVenuesByServiceCodeCaseInsensitive_WithStatusCode_200() throws JsonProcessingException {

        LrdCourtVenuesByServiceCodeResponse response = (LrdCourtVenuesByServiceCodeResponse)
            lrdApiClient.findCourtVenuesByServiceCode("bfa1", LrdCourtVenuesByServiceCodeResponse.class);

        assertThat(response).isNotNull();
        responseVerification(response);
    }

    @Test
    public void returnsCourtVenuesByUnknownServiceCodeWithStatusCode_404() throws JsonProcessingException {

        Map<String, Object> errorResponseMap = (Map<String, Object>)
            lrdApiClient.findCourtVenuesByServiceCode("Invalid Service Code", ErrorResponse.class);


        assertThat(errorResponseMap).containsEntry(HTTP_STATUS, HttpStatus.NOT_FOUND);
    }

    @Test
    public void returnsCourtVenuesByInvalidServiceCodeWithStatusCode_400() throws
        JsonProcessingException {

        Map<String, Object> errorResponseMap = (Map<String, Object>)
            lrdApiClient.findCourtVenuesByServiceCode("@$ABC", ErrorResponse.class);

        assertNotNull(errorResponseMap);
        assertThat(errorResponseMap).containsEntry("http_status", HttpStatus.BAD_REQUEST);
    }

    @Test
    public void returnsCourtVenuesByServiceCode_LDFlagOff_WithStatusCode_403()
        throws Exception {
        Map<String, String> launchDarklyMap = new HashMap<>();
        launchDarklyMap.put(
            "LrdApiController.retrieveBuildingLocationDetails",
            "lrd_location_api"
        );
        when(featureToggleService.isFlagEnabled(anyString(), anyString())).thenReturn(false);
        when(featureToggleService.getLaunchDarklyMap()).thenReturn(launchDarklyMap);
        Map<String, Object> errorResponseMap = (Map<String, Object>)
            lrdApiClient.findCourtVenuesByServiceCode("BFA 1", ErrorResponse.class);
        assertThat(errorResponseMap).containsEntry("http_status", HttpStatus.FORBIDDEN);
        assertThat(((ErrorResponse) errorResponseMap.get("response_body")).getErrorMessage())
            .contains("lrd_location_api".concat(" ").concat(FORBIDDEN_EXCEPTION_LD));
    }

    private void responseVerification(LrdCourtVenuesByServiceCodeResponse response) {
        LrdCourtTypeResponse expectedCourtTypeResponse = new LrdCourtTypeResponse();
        List<LrdCourtVenueResponse> expectedCourtVenueResponses = new ArrayList<>();

        assertThat(response.getServiceCode()).isEqualTo("BFA1");
        assertThat(response.getCourtTypeResponse()).isEqualTo(expectedCourtTypeResponse);
        assertThat(response.getCourtVenueResponses()).isEqualTo(expectedCourtVenueResponses);
    }

}