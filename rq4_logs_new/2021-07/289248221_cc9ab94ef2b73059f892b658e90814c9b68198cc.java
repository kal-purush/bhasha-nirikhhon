package uk.gov.hmcts.reform.lrdapi.controllers;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import uk.gov.hmcts.reform.lrdapi.controllers.advice.InvalidRequestException;
import uk.gov.hmcts.reform.lrdapi.service.ILrdBuildingLocationService;
import uk.gov.hmcts.reform.lrdapi.service.LrdService;
import uk.gov.hmcts.reform.lrdapi.service.RegionService;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class LrdApiControllerRegionDetailsTest {

    @InjectMocks
    private LrdApiController lrdApiController;

    @Mock
    ILrdBuildingLocationService lrdBuildingLocationService;

    @Mock
    LrdService lrdServiceMock;

    @Mock
    RegionService regionServiceMock;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testGetRegionDetails_ByDescription_Returns200() {
        ResponseEntity<Object> responseEntity =
            lrdApiController.retrieveRegionDetails("London", "");

        assertThat(responseEntity).isNotNull();
        assertThat(responseEntity.getStatusCode()).isEqualTo(HttpStatus.OK);
        verify(regionServiceMock, times(1)).retrieveRegionDetails("","London");
    }

    @Test(expected = InvalidRequestException.class)
    public void testGetRegionDetails_ByMissingDescription_Returns400() {
        ResponseEntity<Object> responseEntity =
            lrdApiController.retrieveRegionDetails("af", "213");

        assertThat(responseEntity).isNotNull();
        assertThat(responseEntity.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }
}