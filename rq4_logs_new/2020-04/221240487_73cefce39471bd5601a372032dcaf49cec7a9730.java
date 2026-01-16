package uk.gov.justice.hmpps.datacompliance.services;

import lombok.AllArgsConstructor;
import org.springframework.stereotype.Service;
import uk.gov.justice.hmpps.datacompliance.dto.OffenderNumber;

@Service
@AllArgsConstructor
public class RetentionService {

    private final ManualRetentionService manualRetentionService;

    public boolean isOffenderEligibleForDeletion(final OffenderNumber offenderNumber) {

        final var isManuallyRetained = manualRetentionService.isManuallyRetained(offenderNumber);
        // TODO GDPR-51 complete the following checks
        //  * Pathfinder
        //  * Duplicates
        //  * Moratoria

        return !isManuallyRetained;
    }
}