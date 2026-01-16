package uk.gov.justice.hmpps.datacompliance.services.duplicate.detection;

import org.springframework.stereotype.Service;
import uk.gov.justice.hmpps.datacompliance.dto.OffenderNumber;
import uk.gov.justice.hmpps.datacompliance.repository.jpa.model.duplication.DataDuplicate;
import uk.gov.justice.hmpps.datacompliance.repository.jpa.model.duplication.ImageDuplicate;

import java.util.List;

import static java.util.Collections.emptyList;

@Service
public class DuplicateDetectionService {

    public List<ImageDuplicate> findDuplicatesByImageFor(final OffenderNumber offenderNumber) {
        // TODO GDPR-108 Implement duplicate image detection check
        return emptyList();
    }

    public List<DataDuplicate> findDuplicatesByDataFor(final OffenderNumber offenderNumber) {
        // TODO GDPR-111 Implement duplicate data detection check
        return emptyList();
    }
}