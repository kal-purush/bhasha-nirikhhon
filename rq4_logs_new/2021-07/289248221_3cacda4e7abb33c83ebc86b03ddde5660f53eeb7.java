package uk.gov.hmcts.reform.lrdapi;

import com.fasterxml.jackson.core.JsonProcessingException;
import net.thucydides.core.annotations.WithTag;
import net.thucydides.core.annotations.WithTags;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.ActiveProfiles;
import uk.gov.hmcts.reform.lrdapi.controllers.advice.ErrorResponse;
import uk.gov.hmcts.reform.lrdapi.controllers.response.LrdBuildingLocationResponse;
import uk.gov.hmcts.reform.lrdapi.controllers.response.LrdCourtVenueResponse;
import uk.gov.hmcts.reform.lrdapi.util.CustomSerenityRunner;
import uk.gov.hmcts.reform.lrdapi.util.ToggleEnable;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static uk.gov.hmcts.reform.lrdapi.controllers.constants.LocationRefConstants.NO_COURT_VENUES_FOUND_FOR_CLUSTER_ID;
import static uk.gov.hmcts.reform.lrdapi.controllers.constants.LocationRefConstants.NO_COURT_VENUES_FOUND_FOR_COURT_TYPE_ID;
import static uk.gov.hmcts.reform.lrdapi.controllers.constants.LocationRefConstants.NO_COURT_VENUES_FOUND_FOR_FOR_EPIMMS_ID;
import static uk.gov.hmcts.reform.lrdapi.controllers.constants.LocationRefConstants.NO_COURT_VENUES_FOUND_FOR_REGION_ID;

@RunWith(CustomSerenityRunner.class)
@WithTags({@WithTag("testType:Functional")})
@ActiveProfiles("functional")
public class RetrieveCourtVenueDetailsFunctionalTest extends AuthorizationFunctionalTest {

    private static final String mapKey = "LrdCourtVenueController.retrieveCourtVenues";
    private static final String path = "/court-venues";

    @Test
    @ToggleEnable(mapKey = mapKey, withFeature = true)
    public void shouldRetrieveCourtVenues_For_A_Epimms_Id_WithStatusCode_200() {
        final var response = (LrdCourtVenueResponse[])
            lrdApiClient.retrieveResponseForGivenRequest(HttpStatus.OK, "?epimms_id=815833",
                                                         LrdCourtVenueResponse[].class,
                                                         path
            );
        assertThat(response).isNotEmpty();
        boolean isEachIdMatched = Arrays
            .stream(response)
            .map(LrdCourtVenueResponse::getEpimmsId)
            .allMatch("815833"::equals);
        assertTrue(isEachIdMatched);
    }

    @Test
    @ToggleEnable(mapKey = mapKey, withFeature = true)
    public void shouldRetrieveCourtVenues_For_Multiple_Epimms_Id_WithStatusCode_200() {
        final var response = (LrdCourtVenueResponse[])
            lrdApiClient.retrieveResponseForGivenRequest(HttpStatus.OK, "?epimms_id=815833,219164",
                                                                    LrdCourtVenueResponse[].class, path);
        assertThat(response).isNotEmpty();
        boolean expected = Arrays
            .stream(response)
            .map(LrdCourtVenueResponse::getEpimmsId)
            .allMatch("815833,219164"::contains);
        assertTrue(expected);
    }

    @Test
    @ToggleEnable(mapKey = mapKey, withFeature = true)
    public void shouldRetrieveCourtVenues_For_No_Epimms_Id_WithStatusCode_200() {
        final var response = (LrdCourtVenueResponse[])
            lrdApiClient.retrieveResponseForGivenRequest(HttpStatus.OK, null,
                                                                    LrdCourtVenueResponse[].class, path);
        assertThat(response).isNotNull();
    }

    @Test
    @ToggleEnable(mapKey = mapKey, withFeature = true)
    public void shouldRetrieveCourtVenues_For_All_Epimms_Id_WithStatusCode_200() throws JsonProcessingException {
        final var response = (LrdCourtVenueResponse[])
            lrdApiClient.retrieveResponseForGivenRequest(HttpStatus.OK, "?epimms_id=All",
                                                                    LrdCourtVenueResponse[].class, path);
        assertThat(response).isNotNull();
    }

    @Test
    @ToggleEnable(mapKey = mapKey, withFeature = true)
    public void shouldRetrieveCourtVenues_For_Given_Region_Id_WithStatusCode_200() throws JsonProcessingException {
        final var response = (LrdCourtVenueResponse[])
            lrdApiClient.retrieveResponseForGivenRequest(HttpStatus.OK, "?region_id=7",
                                                                    LrdCourtVenueResponse[].class, path);
        assertThat(response).isNotNull();
        boolean isEveryIdMatched = Arrays
            .stream(response)
            .map(LrdCourtVenueResponse::getRegionId)
            .allMatch("7"::equals);

        assertTrue(isEveryIdMatched);
    }

    @Test
    @ToggleEnable(mapKey = mapKey, withFeature = true)
    public void shouldRetrieveCourtVenues_For_Given_CourtType_Id_WithStatusCode_200() throws JsonProcessingException {
        final var response = (LrdCourtVenueResponse[])
            lrdApiClient.retrieveResponseForGivenRequest(HttpStatus.OK, "?court_type_id=1",
                                                                    LrdCourtVenueResponse[].class, path);
        assertThat(response).isNotNull();
        boolean isEveryIdMatched = Arrays
            .stream(response)
            .map(LrdCourtVenueResponse::getCourtTypeId)
            .allMatch("1"::equals);

        assertTrue(isEveryIdMatched);
    }

    @Test
    @ToggleEnable(mapKey = mapKey, withFeature = true)
    public void shouldRetrieveCourtVenues_For_Given_Cluster_Id_WithStatusCode_200() throws JsonProcessingException {
        final var response = (LrdCourtVenueResponse[])
            lrdApiClient.retrieveResponseForGivenRequest(HttpStatus.OK, "?cluster_id=8",
                                                                    LrdCourtVenueResponse[].class, path);
        assertThat(response).isNotNull();
        boolean isEveryIdMatched = Arrays
            .stream(response)
            .map(LrdCourtVenueResponse::getClusterId)
            .allMatch("8"::equals);
        assertTrue(isEveryIdMatched);

    }


    @Test
    @ToggleEnable(mapKey = mapKey, withFeature = true)
    public void shouldReturn404_WhenNoEpimmsIdFound() {
        final var response = (ErrorResponse)
            lrdApiClient.retrieveResponseForGivenRequest(HttpStatus.NOT_FOUND, "?epimms_id=000000",
                                                                    LrdCourtVenueResponse[].class, path);
        assertEquals(response.getErrorDescription(),
                     String.format(NO_COURT_VENUES_FOUND_FOR_FOR_EPIMMS_ID, "[000000]"));
    }

    @Test
    @ToggleEnable(mapKey = mapKey, withFeature = true)
    public void shouldReturn404_WhenNoRegionIdFound() {
        final var response = (ErrorResponse)
            lrdApiClient.retrieveResponseForGivenRequest(HttpStatus.NOT_FOUND, "?region_id=0",
                                                         LrdCourtVenueResponse[].class, path);
        assertEquals(response.getErrorDescription(),
                     String.format(NO_COURT_VENUES_FOUND_FOR_REGION_ID, "0"));
    }

    @Test
    @ToggleEnable(mapKey = mapKey, withFeature = true)
    public void shouldReturn404_WhenNoClusterIdFound() {
        final var response = (ErrorResponse)
            lrdApiClient.retrieveResponseForGivenRequest(HttpStatus.NOT_FOUND, "?cluster_id=0",
                                                         LrdCourtVenueResponse[].class, path);
        assertEquals(response.getErrorDescription(),
                     String.format(NO_COURT_VENUES_FOUND_FOR_CLUSTER_ID, "0"));
    }

    @Test
    @ToggleEnable(mapKey = mapKey, withFeature = true)
    public void shouldReturn404_WhenNoCourtTypeIdFound() {
        final var response = (ErrorResponse)
            lrdApiClient.retrieveResponseForGivenRequest(HttpStatus.NOT_FOUND, "?court_type_id=000000",
                                                         LrdCourtVenueResponse[].class, path);
        assertEquals(response.getErrorDescription(),
                     String.format(NO_COURT_VENUES_FOUND_FOR_COURT_TYPE_ID, "0"));
    }


    @Test
    @ToggleEnable(mapKey = mapKey, withFeature = false)
    public void shouldNotRetrieveCourtVenues_WhenToggleOff_WithStatusCode_403() {
        ErrorResponse response = (ErrorResponse)
            lrdApiClient
                .retrieveResponseForGivenRequest(HttpStatus.FORBIDDEN,
                                                                  "?epimms_id=815833",
                                                                  LrdBuildingLocationResponse.class, path);
        assertThat(response).isNotNull();
    }
}