package leets.weeth.global.sas.config.grant;

import jakarta.servlet.http.HttpServletRequest;
import org.springframework.security.core.Authentication;
import org.springframework.security.oauth2.core.OAuth2AuthenticationException;
import org.springframework.security.oauth2.core.OAuth2ErrorCodes;
import org.springframework.security.oauth2.core.endpoint.OAuth2ParameterNames;
import org.springframework.security.web.authentication.AuthenticationConverter;
import org.springframework.util.StringUtils;

import java.util.HashMap;

public class KakaoAccessTokenAuthenticationConverter implements AuthenticationConverter {

    @Override
    public Authentication convert(HttpServletRequest request) {
        if (!KakaoGrantType.KAKAO_ACCESS_TOKEN.getValue()
                .equals(request.getParameter(OAuth2ParameterNames.GRANT_TYPE))) {
            return null;
        }

        String kakaoToken = request.getParameter("kakao_token");
        if (!StringUtils.hasText(kakaoToken)) {
            throw new OAuth2AuthenticationException(OAuth2ErrorCodes.INVALID_REQUEST);
        }

        Authentication clientPrincipal =
                (Authentication) request.getUserPrincipal();

        var additional = new HashMap<String, Object>();
        request.getParameterMap().forEach((k, v) -> {
            if (!OAuth2ParameterNames.GRANT_TYPE.equals(k) && !"kakao_token".equals(k))
                additional.put(k, v[0]);
        });

        return new KakaoAccessTokenAuthenticationToken(kakaoToken, clientPrincipal, additional);
    }
}