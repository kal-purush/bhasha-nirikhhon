package uk.gov.hmcts.reform.lrdapi;

import net.thucydides.core.annotations.WithTag;
import net.thucydides.core.annotations.WithTags;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.ActiveProfiles;
import uk.gov.hmcts.reform.lrdapi.controllers.advice.ErrorResponse;
import uk.gov.hmcts.reform.lrdapi.controllers.response.LrdCourtTypeResponse;
import uk.gov.hmcts.reform.lrdapi.controllers.response.LrdCourtVenueResponse;
import uk.gov.hmcts.reform.lrdapi.controllers.response.LrdCourtVenuesByServiceCodeResponse;
import uk.gov.hmcts.reform.lrdapi.util.CustomSerenityRunner;
import uk.gov.hmcts.reform.lrdapi.util.ToggleEnable;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static uk.gov.hmcts.reform.lrdapi.util.CustomSerenityRunner.getFeatureFlagName;
import static uk.gov.hmcts.reform.lrdapi.util.FeatureConditionEvaluation.FORBIDDEN_EXCEPTION_LD;

@RunWith(CustomSerenityRunner.class)
@WithTags({@WithTag("testType:Functional")})
@ActiveProfiles("functional")
public class RetrieveCourtVenuesByServiceCodeFunctionalTest extends AuthorizationFunctionalTest{

    public static final String mapKey = "LrdApiController.retrieveCourtVenuesByServiceCode";

    @Test
    @ToggleEnable(mapKey = mapKey, withFeature = true)
    public void getCourtVenuesByServiceCodeWithStatusCode_200() {
        LrdCourtVenuesByServiceCodeResponse response = (LrdCourtVenuesByServiceCodeResponse)
            lrdApiClient.retrieveCourtVenuesByServiceCode(HttpStatus.OK, "BFA1");

        assertThat(response).isNotNull();
        responseVerification(response);
    }


    @Test
    @ToggleEnable(mapKey = mapKey, withFeature = false)
    public void should_retrieve_403_when_Api_toggled_off() {
        String exceptionMessage = getFeatureFlagName().concat(" ").concat(FORBIDDEN_EXCEPTION_LD);
        validateErrorResponse(
            (ErrorResponse) lrdApiClient.retrieveCourtVenuesByServiceCode(
                HttpStatus.FORBIDDEN, ""),
            exceptionMessage,
            exceptionMessage
        );
    }


    private void responseVerification(LrdCourtVenuesByServiceCodeResponse response) {
        LrdCourtTypeResponse expectedCourtTypeResponse = new LrdCourtTypeResponse();
        List<LrdCourtVenueResponse> expectedCourtVenueResponses = new ArrayList<>();

        assertThat(response.getServiceCode()).isEqualTo("BFA1");
        assertThat(response.getCourtTypeResponse()).isEqualTo(expectedCourtTypeResponse);
        assertThat(response.getCourtVenueResponses()).isEqualTo(expectedCourtVenueResponses);
    }

}