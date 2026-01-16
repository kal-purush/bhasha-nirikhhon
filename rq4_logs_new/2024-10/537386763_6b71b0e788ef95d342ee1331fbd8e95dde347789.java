package it.pagopa.pn.national.registries.config;

import io.netty.handler.timeout.TimeoutException;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import org.springframework.web.reactive.function.client.WebClientResponseException;

import javax.net.ssl.SSLHandshakeException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;

@Slf4j
@Component
@Data
public class CustomRetryConfig {

    private final int retryMaxAttempts;

    public CustomRetryConfig(@Value("${pn.national.registries.custom.retry.max-attempts}") int retryMaxAttempts) {
        this.retryMaxAttempts = retryMaxAttempts;
    }

    public static boolean isRetryableException(Throwable throwable) {
        return throwable instanceof TimeoutException || throwable instanceof SocketException || throwable instanceof SocketTimeoutException || throwable instanceof SSLHandshakeException || throwable instanceof UnknownHostException || throwable instanceof WebClientRequestException || throwable instanceof WebClientResponseException.TooManyRequests || throwable instanceof WebClientResponseException.GatewayTimeout || throwable instanceof WebClientResponseException.BadGateway || throwable instanceof WebClientResponseException.ServiceUnavailable;
    }
}