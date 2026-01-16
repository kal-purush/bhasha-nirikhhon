package com.justdo.plug.auth.global.jwt.kakao.preVersion;

import com.justdo.plug.auth.global.exception.ApiException;
import com.justdo.plug.auth.global.jwt.kakao.preVersion.dto.response.KakaoTokenResponse;
import com.justdo.plug.auth.global.response.code.status.ErrorStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Flux;

@Component
@RequiredArgsConstructor
@Slf4j
public class KakaoTokenJsonData {
    private final WebClient webClient = WebClient.create();

    @Value("${kakao.token_uri}")
    private String tokenUri;

    @Value("${kakao.redirect_uri}")
    private String redirectUri;

    @Value("${kakao.grant_type}")
    private String grantType;

    @Value("${kakao.client_id}")
    private String clientId;

    public KakaoTokenResponse getToken(String code) {
        String uri = tokenUri + "?grant_type=" + grantType + "&client_id="
                + clientId + "&redirect_uri=" + redirectUri + "&code=" + code;

        log.info("uri = {}", uri);

        Flux<KakaoTokenResponse> response = webClient.post()
                .uri(uri)
                .contentType(MediaType.APPLICATION_JSON)
                .retrieve()
                .bodyToFlux(KakaoTokenResponse.class);

        return response.onErrorMap(WebClientResponseException.class,
                e -> new ApiException(ErrorStatus._KAKAO_TOKEN_ERROR)).blockFirst();
    }

}