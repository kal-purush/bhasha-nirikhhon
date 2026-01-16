package com.joboffers.http.error;

import com.github.dockerjava.zerodep.shaded.org.apache.hc.core5.http.HttpStatus;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.http.Fault;
import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import com.joboffers.domain.offer.OfferFetchRepository;
import com.joboffers.infrastructure.offer.client.OfferFetcherRepositoryConfigurationProperties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.web.server.ResponseStatusException;

import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

public class OffersRestTemplateErrorsIntegrationTest {



    @RegisterExtension
    public static WireMockExtension wireMockServer = WireMockExtension.newInstance()
            .options(wireMockConfig().dynamicPort())
            .build();
    OfferFetcherRepositoryConfigurationProperties properties = OfferFetcherRepositoryConfigurationProperties.builder()
            .uri("http://localhost")
            .port(wireMockServer.getPort())
            .connectionTimeout(1000)
            .readTimeout(1000)
            .build();
    OfferFetchRepository offerFetchRepository = new OffersRestTemplateTestConfig(properties).remoteOfferClient();



    @Test
    void should_return_500_internal_server_error_when_fault_connection_reset_by_peer(){
        // given
        wireMockServer.stubFor(WireMock.get("/offers")
                .willReturn(WireMock.aResponse()
                        .withStatus(HttpStatus.SC_OK)
                        .withHeader("Content-Type", "application/json")
                        .withFault(Fault.CONNECTION_RESET_BY_PEER)
                ));

        // when
        Throwable throwable = catchThrowable(()->offerFetchRepository.fetchAllOffers());

        // then
        assertThat(throwable).isInstanceOf(ResponseStatusException.class);
        assertThat(throwable.getMessage()).isEqualTo("500 INTERNAL_SERVER_ERROR");

    }

}