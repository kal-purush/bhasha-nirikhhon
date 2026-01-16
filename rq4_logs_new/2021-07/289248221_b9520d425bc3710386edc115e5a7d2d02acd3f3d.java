package uk.gov.hmcts.reform.lrdapi.repository;

import org.junit.Test;
import uk.gov.hmcts.reform.lrdapi.domain.CourtTypeServiceAssoc;

import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CourtTypeServiceAssocRepositoryTest {

    public CourtTypeServiceAssocRepository courtTypeServiceAssocRepository = mock(CourtTypeServiceAssocRepository.class);

    @Test
    public void findCourtVenuesByServiceCodeTest() {
        when(courtTypeServiceAssocRepository.findByServiceCode(anyString())).thenReturn(new CourtTypeServiceAssoc());
        assertNotNull(courtTypeServiceAssocRepository.findByServiceCode(anyString()));
    }
}