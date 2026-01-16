package leets.weeth.global.sas.application.mapper;


import leets.weeth.global.sas.config.grant.KakaoGrantType;
import leets.weeth.global.sas.domain.entity.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.security.oauth2.core.AuthorizationGrantType;
import org.springframework.security.oauth2.core.OAuth2AccessToken;
import org.springframework.security.oauth2.core.OAuth2RefreshToken;
import org.springframework.security.oauth2.core.endpoint.OAuth2AuthorizationRequest;
import org.springframework.security.oauth2.core.oidc.OidcIdToken;
import org.springframework.security.oauth2.core.oidc.OidcScopes;
import org.springframework.security.oauth2.server.authorization.OAuth2Authorization;
import org.springframework.security.oauth2.server.authorization.OAuth2AuthorizationCode;
import org.springframework.security.oauth2.server.authorization.settings.OAuth2TokenFormat;

import java.security.Principal;
import java.util.Map;

@Slf4j
public class ModelMapper {
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

    static AuthorizationCode extractAuthorizationCode(OAuth2Authorization authorization) {
        AuthorizationCode authorizationCode = null;

        if (authorization.getToken(OAuth2AuthorizationCode.class) != null) {
            OAuth2Authorization.Token<OAuth2AuthorizationCode> oauth2AuthorizationCode = authorization.getToken(OAuth2AuthorizationCode.class);

            authorizationCode = AuthorizationCode.builder()
                    .tokenValue(oauth2AuthorizationCode.getToken().getTokenValue())
                    .issuedAt(oauth2AuthorizationCode.getToken().getIssuedAt())
                    .expiresAt(oauth2AuthorizationCode.getToken().getExpiresAt())
                    .invalidated(oauth2AuthorizationCode.isInvalidated())
                    .build();
        }

        return authorizationCode;
    }

    static AccessToken extractAccessToken(OAuth2Authorization authorization) {
        AccessToken accessToken = null;

        if (authorization.getAccessToken() != null) {
            OAuth2Authorization.Token<OAuth2AccessToken> oauth2AccessToken = authorization.getAccessToken();
            OAuth2TokenFormat tokenFormat = null;

            if (OAuth2TokenFormat.SELF_CONTAINED.getValue().equals(oauth2AccessToken.getMetadata(OAuth2TokenFormat.class.getName()))) {
                tokenFormat = OAuth2TokenFormat.SELF_CONTAINED;
            } else if (OAuth2TokenFormat.REFERENCE.getValue().equals(oauth2AccessToken.getMetadata(OAuth2TokenFormat.class.getName()))) {
                tokenFormat = OAuth2TokenFormat.REFERENCE;
            }
            accessToken = AccessToken.builder()
                    .tokenValue(oauth2AccessToken.getToken().getTokenValue())
                    .issuedAt(oauth2AccessToken.getToken().getIssuedAt())
                    .expiresAt(oauth2AccessToken.getToken().getExpiresAt())
                    .invalidated(oauth2AccessToken.isInvalidated())
                    .tokenType(oauth2AccessToken.getToken().getTokenType())
                    .scopes(oauth2AccessToken.getToken().getScopes())
                    .tokenFormat(tokenFormat)
                    .claims(ClaimsHolder.builder()
                            .claims(oauth2AccessToken.getClaims() != null ? oauth2AccessToken.getClaims() : Map.of())
                            .build())
                    .build();
        }
        return accessToken;
    }

    static RefreshToken extractRefreshToken(OAuth2Authorization authorization) {
        RefreshToken refreshToken = null;
        if (authorization.getRefreshToken() != null) {
            OAuth2Authorization.Token<OAuth2RefreshToken> oauth2RefreshToken = authorization.getRefreshToken();

            refreshToken = RefreshToken.builder()
                    .tokenValue(oauth2RefreshToken.getToken().getTokenValue())
                    .issuedAt(oauth2RefreshToken.getToken().getIssuedAt())
                    .expiresAt(oauth2RefreshToken.getToken().getExpiresAt())
                    .invalidated(oauth2RefreshToken.isInvalidated())
                    .build();
        }

        return refreshToken;
    }

    static IdToken extractIdToken(OAuth2Authorization authorization) {
        IdToken idToken = null;
        if (authorization.getToken(OidcIdToken.class) != null) {
            OAuth2Authorization.Token<OidcIdToken> oidcIdToken = authorization.getToken(OidcIdToken.class);
            idToken = IdToken.builder()
                    .tokenValue(oidcIdToken.getToken().getTokenValue())
                    .issuedAt(oidcIdToken.getToken().getIssuedAt())
                    .expiresAt(oidcIdToken.getToken().getExpiresAt())
                    .invalidated(oidcIdToken.isInvalidated())
                    .claims(ClaimsHolder.builder()
                            .claims(oidcIdToken.getClaims() != null ? oidcIdToken.getClaims() : Map.of())
                            .build())
                    .build();
        }
        return idToken;
    }

    public static void mapOAuth2AuthorizationGrantAuthorization(
            OAuth2AuthorizationGrantAuthorization authorizationGrantAuthorization,
            OAuth2Authorization.Builder builder) {

        if (authorizationGrantAuthorization instanceof OidcAuthorizationCodeGrantAuthorization authorizationGrant) {
            mapOidcAuthorizationCodeGrantAuthorization(authorizationGrant, builder);
        } else if (authorizationGrantAuthorization instanceof OAuth2AuthorizationCodeGrantAuthorization authorizationGrant) {
            mapOAuth2AuthorizationCodeGrantAuthorization(authorizationGrant, builder);
        }
    }

    static void mapOidcAuthorizationCodeGrantAuthorization(
            OidcAuthorizationCodeGrantAuthorization authorizationCodeGrantAuthorization,
            OAuth2Authorization.Builder builder) {

        mapOAuth2AuthorizationCodeGrantAuthorization(authorizationCodeGrantAuthorization, builder);
        mapIdToken(authorizationCodeGrantAuthorization.getIdToken(), builder);
    }

    static void mapOAuth2AuthorizationCodeGrantAuthorization(
            OAuth2AuthorizationCodeGrantAuthorization authorizationCodeGrantAuthorization,
            OAuth2Authorization.Builder builder) {

        builder.id(authorizationCodeGrantAuthorization.getId())
                .principalName(authorizationCodeGrantAuthorization.getPrincipalName())
                .authorizationGrantType(AuthorizationGrantType.AUTHORIZATION_CODE)
                .authorizedScopes(authorizationCodeGrantAuthorization.getAuthorizedScopes());

        if (authorizationCodeGrantAuthorization.getPrincipal() != null) {
            builder.attribute(Principal.class.getName(), authorizationCodeGrantAuthorization.getPrincipal());
        }

        if (authorizationCodeGrantAuthorization.getAuthorizationRequest() != null) {
            builder.attribute(OAuth2AuthorizationRequest.class.getName(),
                    authorizationCodeGrantAuthorization.getAuthorizationRequest());
        }

        mapAuthorizationCode(authorizationCodeGrantAuthorization.getAuthorizationCode(), builder);
        mapAccessToken(authorizationCodeGrantAuthorization.getAccessToken(), builder);
        mapRefreshToken(authorizationCodeGrantAuthorization.getRefreshToken(), builder);
    }

    static void mapAuthorizationCode(AuthorizationCode authorizationCode,
                                     OAuth2Authorization.Builder builder) {
        if (authorizationCode == null) {
            return;
        }
        OAuth2AuthorizationCode oauth2AuthorizationCode = new OAuth2AuthorizationCode(authorizationCode.getTokenValue(),
                authorizationCode.getIssuedAt(), authorizationCode.getExpiresAt());
        builder.token(oauth2AuthorizationCode, (metadata) -> metadata
                .put(OAuth2Authorization.Token.INVALIDATED_METADATA_NAME, authorizationCode.isInvalidated()));
    }

    static void mapAccessToken(AccessToken accessToken,
                               OAuth2Authorization.Builder builder) {
        if (accessToken == null) {
            return;
        }
        OAuth2AccessToken oauth2AccessToken = new OAuth2AccessToken(accessToken.getTokenType(),
                accessToken.getTokenValue(), accessToken.getIssuedAt(), accessToken.getExpiresAt(),
                accessToken.getScopes());
        builder.token(oauth2AccessToken, (metadata) -> {
            metadata.put(OAuth2Authorization.Token.INVALIDATED_METADATA_NAME, accessToken.isInvalidated());
            metadata.put(OAuth2Authorization.Token.CLAIMS_METADATA_NAME, accessToken.getClaims().getClaims());
            metadata.put(OAuth2TokenFormat.class.getName(), accessToken.getTokenFormat().getValue());
        });
    }

    static void mapRefreshToken(RefreshToken refreshToken,
                                OAuth2Authorization.Builder builder) {
        if (refreshToken == null) {
            return;
        }
        OAuth2RefreshToken oauth2RefreshToken = new OAuth2RefreshToken(refreshToken.getTokenValue(),
                refreshToken.getIssuedAt(), refreshToken.getExpiresAt());
        builder.token(oauth2RefreshToken, (metadata) -> metadata
                .put(OAuth2Authorization.Token.INVALIDATED_METADATA_NAME, refreshToken.isInvalidated()));
    }

    static void mapIdToken(IdToken idToken, OAuth2Authorization.Builder builder) {
        if (idToken == null) {
            return;
        }
        OidcIdToken oidcIdToken = new OidcIdToken(idToken.getTokenValue(), idToken.getIssuedAt(),
                idToken.getExpiresAt(), idToken.getClaims().getClaims());
        builder.token(oidcIdToken, (metadata) -> {
            metadata.put(OAuth2Authorization.Token.INVALIDATED_METADATA_NAME, idToken.isInvalidated());
            metadata.put(OAuth2Authorization.Token.CLAIMS_METADATA_NAME, idToken.getClaims().getClaims());
        });
    }

}