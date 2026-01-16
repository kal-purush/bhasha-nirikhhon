package leets.weeth.global.sas.config.authentication.strategy;

import leets.weeth.global.sas.application.property.OauthProviderProperties;
import org.springframework.stereotype.Component;
import org.springframework.web.util.UriComponentsBuilder;

@Component("kakao")
public class KakaoUrlBuilder implements CustomOauthUrlBuilder {
    @Override
    public String buildOauthUrl(OauthProviderProperties.Provider cfg) {
        return UriComponentsBuilder.fromHttpUrl(cfg.getAuthorizeUri())
                .queryParam("client_id", cfg.getClientId())
                .queryParam("redirect_uri", cfg.getRedirectUri())
                .queryParam("response_type", "code")
                .build()
                .encode()
                .toUriString();
    }
}