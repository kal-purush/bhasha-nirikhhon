package pl.lotto.http.numbergenerator;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.http.Fault;
import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.web.server.ResponseStatusException;
import pl.lotto.domain.numbergenerator.WinningNumbersGenerable;
import pl.lotto.infrastructure.numbergenerator.http.RandomNumberGeneratorRestTemplateConfigurationProperties;
import wiremock.org.apache.hc.core5.http.HttpStatus;

import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

public class RandomNumberGeneratorRestTemplateErrorsIntegrationTest {

    @RegisterExtension
    public static WireMockExtension wireMockServer = WireMockExtension.newInstance()
            .options(wireMockConfig().dynamicPort())
            .build();

    WinningNumbersGenerable winningNumbersGenerable = new RandomNumberGeneratorRestTemplateIntegrationTestConfig().remoteNumberGeneratorClient(
            RandomNumberGeneratorRestTemplateConfigurationProperties.builder()
                    .uri("http://localhost")
                    .port(wireMockServer.getPort())
                    .connectionTimeout(1000)
                    .readTimeout(1000)
                    .build()
    );

    @Test
    void should_throw_500_internal_server_error_when_fault_connection_reset_by_peer(){
        // given
        wireMockServer.stubFor(WireMock.get("/api/v1.0/randomnumber?min=1&max=99&count=14")
                .willReturn(WireMock.aResponse()
                        .withStatus(HttpStatus.SC_OK)
                        .withHeader("Content-Type", "application/json")
                        .withFault(Fault.CONNECTION_RESET_BY_PEER)));

        // when
        Throwable throwable = catchThrowable(() -> winningNumbersGenerable.generateSixRandomNumbers(1, 99, 14));

        // then
        assertThat(throwable).isInstanceOf(ResponseStatusException.class);
        assertThat(throwable.getMessage()).isEqualTo("500 INTERNAL_SERVER_ERROR");
    }

    @Test
    void should_throw_500_internal_server_error_when_fault_empty_response() {
        // given
        wireMockServer.stubFor(WireMock.get("/api/v1.0/randomnumber?min=1&max=99&count=14")
                .willReturn(WireMock.aResponse()
                        .withStatus(HttpStatus.SC_OK)
                        .withHeader("Content-Type", "application/json")
                        .withFault(Fault.EMPTY_RESPONSE)));

        // when
        Throwable throwable = catchThrowable(() -> winningNumbersGenerable.generateSixRandomNumbers(1, 99, 14));

        // then
        assertThat(throwable).isInstanceOf(ResponseStatusException.class);
        assertThat(throwable.getMessage()).isEqualTo("500 INTERNAL_SERVER_ERROR");
    }

    @Test
    void should_throw_500_internal_server_error_when_fault_malformed_response_chunk() {
        // given
        wireMockServer.stubFor(WireMock.get("/api/v1.0/randomnumber?min=1&max=99&count=14")
                .willReturn(WireMock.aResponse()
                        .withStatus(HttpStatus.SC_OK)
                        .withHeader("Content-Type", "application/json")
                        .withFault(Fault.MALFORMED_RESPONSE_CHUNK)));
        // when
        Throwable throwable = catchThrowable(() -> winningNumbersGenerable.generateSixRandomNumbers(1, 99, 14));

        // then
        assertThat(throwable).isInstanceOf(ResponseStatusException.class);
        assertThat(throwable.getMessage()).isEqualTo("500 INTERNAL_SERVER_ERROR");
    }

    @Test
    void should_throw_500_internal_server_error_when_fault_random_data_then_close() {
        // given
        wireMockServer.stubFor(WireMock.get("/api/v1.0/randomnumber?min=1&max=99&count=14")
                .willReturn(WireMock.aResponse()
                        .withStatus(HttpStatus.SC_OK)
                        .withHeader("Content-Type", "application/json")
                        .withFault(Fault.RANDOM_DATA_THEN_CLOSE)));

        // when
        Throwable throwable = catchThrowable(() -> winningNumbersGenerable.generateSixRandomNumbers(1, 99, 14));

        // then
        assertThat(throwable).isInstanceOf(ResponseStatusException.class);
        assertThat(throwable.getMessage()).isEqualTo("500 INTERNAL_SERVER_ERROR");
    }

    @Test
    void should_throw_204_no_content_when_status_is_204_no_content() {
        // given
        wireMockServer.stubFor(WireMock.get("/api/v1.0/randomnumber?min=1&max=99&count=14")
                .willReturn(WireMock.aResponse()
                        .withStatus(HttpStatus.SC_NO_CONTENT)
                        .withHeader("Content-Type", "application/json")
                        .withBody("""
                                [1, 2, 3, 4, 5, 6, 82, 56, 87, 93, 64, 23, 15, 60]
                                          """.trim()
                        )));

        // when
        Throwable throwable = catchThrowable(() -> winningNumbersGenerable.generateSixRandomNumbers(1, 99, 14));

        // then
        assertThat(throwable).isInstanceOf(ResponseStatusException.class);
        assertThat(throwable.getMessage()).isEqualTo("204 NO_CONTENT");
    }

    @Test
    void should_throw_500_internal_server_error_when_response_delay_is_5000_ms_and_client_has_1000ms_read_timeout() {
        // given
        wireMockServer.stubFor(WireMock.get("/api/v1.0/randomnumber?min=1&max=99&count=14")
                .willReturn(WireMock.aResponse()
                        .withStatus(HttpStatus.SC_OK)
                        .withHeader("Content-Type", "application/json")
                        .withBody("""
                                [1, 2, 3, 4, 5, 6, 82, 56, 87, 93, 64, 23, 15, 60]
                                          """.trim()
                        )
                        .withFixedDelay(5000)));

        // when
        Throwable throwable = catchThrowable(() -> winningNumbersGenerable.generateSixRandomNumbers(1, 99, 14));

        // then
        assertThat(throwable).isInstanceOf(ResponseStatusException.class);
        assertThat(throwable.getMessage()).isEqualTo("500 INTERNAL_SERVER_ERROR");
    }

    @Test
    void should_404_not_found_when_http_service_returning_not_found_status() {
        // given
        wireMockServer.stubFor(WireMock.get("/api/v1.0/randomnumber?min=1&max=99&count=14")
                .willReturn(WireMock.aResponse()
                        .withHeader("Content-Type", "application/json")
                        .withStatus(HttpStatus.SC_NOT_FOUND))
        );

        // when
        Throwable throwable = catchThrowable(() -> winningNumbersGenerable.generateSixRandomNumbers(1, 99, 14));

        // then
        assertThat(throwable).isInstanceOf(ResponseStatusException.class);
        assertThat(throwable.getMessage()).isEqualTo("404 NOT_FOUND");
    }

    @Test
    void should_throw_401_unauthorized_when_http_service_returning_unauthorized_status() {
        // given
        wireMockServer.stubFor(WireMock.get("/api/v1.0/randomnumber?min=1&max=99&count=14")
                .willReturn(WireMock.aResponse()
                        .withHeader("Content-Type", "application/json")
                        .withStatus(HttpStatus.SC_UNAUTHORIZED))
        );

        // when
        Throwable throwable = catchThrowable(() -> winningNumbersGenerable.generateSixRandomNumbers(1, 99, 14));

        // then
        assertThat(throwable).isInstanceOf(ResponseStatusException.class);
        assertThat(throwable.getMessage()).isEqualTo("401 UNAUTHORIZED");
    }
}