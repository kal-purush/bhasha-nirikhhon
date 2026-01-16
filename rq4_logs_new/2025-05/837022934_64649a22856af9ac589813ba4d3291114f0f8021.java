package leets.weeth.global.sas.config.grant;

import lombok.Getter;
import org.springframework.security.core.Authentication;
import org.springframework.security.oauth2.core.AuthorizationGrantType;
import org.springframework.security.oauth2.server.authorization.authentication.OAuth2AuthorizationGrantAuthenticationToken;

import java.util.Map;

@Getter
public class KakaoAccessTokenAuthenticationToken extends OAuth2AuthorizationGrantAuthenticationToken {

    private final String kakaoAccessToken;

    public KakaoAccessTokenAuthenticationToken(String kakaoAccessToken, Authentication clientPrincipal, Map<String, Object> additionalParameters) {
        super(new AuthorizationGrantType("kakao_access_token"), clientPrincipal, additionalParameters);
        this.kakaoAccessToken = kakaoAccessToken;
    }

}