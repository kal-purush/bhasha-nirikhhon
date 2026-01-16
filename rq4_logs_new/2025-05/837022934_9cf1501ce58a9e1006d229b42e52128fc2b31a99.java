package leets.weeth.global.sas.application.mapper;


import leets.weeth.global.sas.config.grant.KakaoGrantType;
import leets.weeth.global.sas.domain.entity.*;
import org.springframework.security.oauth2.core.AuthorizationGrantType;
import org.springframework.security.oauth2.core.endpoint.OAuth2AuthorizationRequest;
import org.springframework.security.oauth2.core.oidc.OidcScopes;
import org.springframework.security.oauth2.server.authorization.OAuth2Authorization;

import java.security.Principal;

import static leets.weeth.global.sas.application.mapper.Oauth2AuthorizationTokenExtractor.*;

public class OAuth2AuthorizationConverter {
    public static OAuth2AuthorizationGrantAuthorization convertOAuth2AuthorizationGrantAuthorization(OAuth2Authorization authorization) {
        AuthorizationGrantType grantType = authorization.getAuthorizationGrantType();

        if (AuthorizationGrantType.AUTHORIZATION_CODE.equals(grantType)) {
            OAuth2AuthorizationRequest req =
                    authorization.getAttribute(OAuth2AuthorizationRequest.class.getName());

            if (req == null) {
                return convertKakaoAuthorizationGrantAuthorization(authorization);
            }

            return req.getScopes().contains(OidcScopes.OPENID)
                    ? convertOidcAuthorizationCodeGrantAuthorization(authorization)
                    : convertOAuth2AuthorizationCodeGrantAuthorization(authorization);
        }

        if (KakaoGrantType.KAKAO_ACCESS_TOKEN.equals(grantType)) {
            return convertKakaoAuthorizationGrantAuthorization(authorization);
        }

        return null;
    }

    private static OidcAuthorizationCodeGrantAuthorization
    convertKakaoAuthorizationGrantAuthorization(OAuth2Authorization authorization) {

        AccessToken accessToken = extractAccessToken(authorization);
        RefreshToken refreshToken = extractRefreshToken(authorization);
        IdToken idToken = extractIdToken(authorization);

        return OidcAuthorizationCodeGrantAuthorization.builder()
                .id(authorization.getId())
                .registeredClientId(authorization.getRegisteredClientId())
                .principalName(authorization.getPrincipalName())
                .authorizedScopes(authorization.getAuthorizedScopes())
                .accessToken(accessToken)
                .refreshToken(refreshToken)
                .idToken(idToken)
                .principal(authorization.getAttribute(Principal.class.getName()))
                .build();
    }

    static OidcAuthorizationCodeGrantAuthorization convertOidcAuthorizationCodeGrantAuthorization(OAuth2Authorization authorization) {
        AuthorizationCode authorizationCode = extractAuthorizationCode(authorization);
        AccessToken accessToken = extractAccessToken(authorization);
        RefreshToken refreshToken = extractRefreshToken(authorization);
        IdToken idToken = extractIdToken(authorization);

        return OidcAuthorizationCodeGrantAuthorization.builder()
                .id(authorization.getId())
                .registeredClientId(authorization.getRegisteredClientId())
                .principalName(authorization.getPrincipalName())
                .authorizedScopes(authorization.getAuthorizedScopes())
                .accessToken(accessToken)
                .refreshToken(refreshToken)
                .authorizationCode(authorizationCode)
                .idToken(idToken)
                .principal(authorization.getAttribute(Principal.class.getName()))
                .authorizationRequest(authorization.getAttribute(OAuth2AuthorizationRequest.class.getName()))
                .build();
    }

    static OAuth2AuthorizationCodeGrantAuthorization convertOAuth2AuthorizationCodeGrantAuthorization(OAuth2Authorization authorization) {

        AuthorizationCode authorizationCode = extractAuthorizationCode(authorization);
        AccessToken accessToken = extractAccessToken(authorization);
        RefreshToken refreshToken = extractRefreshToken(authorization);

        return OAuth2AuthorizationCodeGrantAuthorization.builder()
                .id(authorization.getId())
                .registeredClientId(authorization.getRegisteredClientId())
                .principalName(authorization.getPrincipalName())
                .authorizedScopes(authorization.getAuthorizedScopes())
                .accessToken(accessToken)
                .refreshToken(refreshToken)
                .principal(authorization.getAttribute(Principal.class.getName()))
                .authorizationRequest(authorization.getAttribute(OAuth2AuthorizationRequest.class.getName()))
                .authorizationCode(authorizationCode)
                .build();
    }
}