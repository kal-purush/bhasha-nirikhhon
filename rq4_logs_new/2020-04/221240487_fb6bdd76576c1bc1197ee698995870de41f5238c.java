package uk.gov.justice.hmpps.datacompliance.services.duplicate.detection;

import org.junit.jupiter.api.Test;
import uk.gov.justice.hmpps.datacompliance.dto.OffenderNumber;

import static org.assertj.core.api.Assertions.assertThat;

class DuplicateDetectionServiceTest {

    private static final OffenderNumber OFFENDER_NUMBER = new OffenderNumber("A1234AA");

    private final DuplicateDetectionService service = new DuplicateDetectionService();

    @Test
    void findDuplicatesByImageReturnsEmpty() {
        assertThat(service.findDuplicatesByImageFor(OFFENDER_NUMBER)).isEmpty();
    }

    @Test
    void findDuplicatesByDataReturnsEmpty() {
        assertThat(service.findDuplicatesByDataFor(OFFENDER_NUMBER)).isEmpty();
    }
}