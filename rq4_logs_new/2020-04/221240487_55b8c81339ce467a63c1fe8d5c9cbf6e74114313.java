package uk.gov.justice.hmpps.datacompliance.client.pathfinder;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import uk.gov.justice.hmpps.datacompliance.config.DataComplianceProperties;
import uk.gov.justice.hmpps.datacompliance.dto.OffenderNumber;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.springframework.http.HttpStatus.NOT_FOUND;

@Service
public class PathfinderApiClient {

    private static final String PATHFINDER_PATH = "/pathfinder/offender/%s";

    private final WebClient webClient;
    private final DataComplianceProperties dataComplianceProperties;

    public PathfinderApiClient(@Qualifier("authorizedWebClient") final WebClient webClient,
                               final DataComplianceProperties dataComplianceProperties) {
        this.webClient = webClient;
        this.dataComplianceProperties = dataComplianceProperties;
    }

    public boolean isReferredToPathfinder(final OffenderNumber offenderNumber) {
        final var url = dataComplianceProperties.getPathfinderApiBaseUrl() +
                format(PATHFINDER_PATH, offenderNumber.getOffenderNumber());

        final var response = webClient.get()
                .uri(url)
                .retrieve()

                // Not found should not generate an exception:
                .onStatus(NOT_FOUND::equals, ignored -> Mono.empty())

                .toBodilessEntity()
                .block(dataComplianceProperties.getPathfinderApiTimeout());

        return requireNonNull(response).getStatusCode().is2xxSuccessful();
    }
}