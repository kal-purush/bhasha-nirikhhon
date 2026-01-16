package uk.gov.hmcts.reform.lrdapi.controllers;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import uk.gov.hmcts.reform.lrdapi.controllers.advice.InvalidRequestException;
import uk.gov.hmcts.reform.lrdapi.service.CourtVenueService;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class LrdCourtVenueControllerTest {

    @InjectMocks
    LrdCourtVenueController lrdCourtVenueController;

    @Mock
    CourtVenueService courtVenueServiceMock;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testGetCourtVenues_ByServiceCode_Returns200() {
        ResponseEntity<Object> responseEntity =
            lrdCourtVenueController.retrieveCourtVenuesByServiceCode("BFA1");

        assertThat(responseEntity).isNotNull();
        assertThat(responseEntity.getStatusCode()).isEqualTo(HttpStatus.OK);
        verify(courtVenueServiceMock, times(1)).retrieveCourtVenuesByServiceCode("BFA1");
    }

    @Test(expected = InvalidRequestException.class)
    public void testGetCourtVenues_ByBlankServiceCode_Returns400() {
        ResponseEntity<Object> responseEntity =
            lrdCourtVenueController.retrieveCourtVenuesByServiceCode("");

        assertThat(responseEntity).isNotNull();
        assertThat(responseEntity.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test(expected = InvalidRequestException.class)
    public void testGetCourtVenues_ByInvalidServiceCode_Returns400() {
        ResponseEntity<Object> responseEntity =
            lrdCourtVenueController.retrieveCourtVenuesByServiceCode("@AB_C");

        assertThat(responseEntity).isNotNull();
        assertThat(responseEntity.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }
}