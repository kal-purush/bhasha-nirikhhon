package leets.weeth.global.sas.config.grant;

import leets.weeth.domain.user.domain.entity.SecurityUser;
import leets.weeth.domain.user.domain.entity.User;
import lombok.RequiredArgsConstructor;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.oauth2.core.*;
import org.springframework.security.oauth2.core.oidc.OidcIdToken;
import org.springframework.security.oauth2.core.oidc.OidcScopes;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.server.authorization.OAuth2Authorization;
import org.springframework.security.oauth2.server.authorization.OAuth2Authorization.Token;
import org.springframework.security.oauth2.server.authorization.OAuth2AuthorizationService;
import org.springframework.security.oauth2.server.authorization.OAuth2TokenType;
import org.springframework.security.oauth2.server.authorization.authentication.OAuth2AccessTokenAuthenticationToken;
import org.springframework.security.oauth2.server.authorization.authentication.OAuth2ClientAuthenticationToken;
import org.springframework.security.oauth2.server.authorization.client.RegisteredClient;
import org.springframework.security.oauth2.server.authorization.context.AuthorizationServerContextHolder;
import org.springframework.security.oauth2.server.authorization.settings.OAuth2TokenFormat;
import org.springframework.security.oauth2.server.authorization.token.DefaultOAuth2TokenContext;
import org.springframework.security.oauth2.server.authorization.token.OAuth2TokenGenerator;

import java.security.Principal;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@RequiredArgsConstructor
public abstract class CustomAuthenticationProvider<T> implements AuthenticationProvider {

    private static final OAuth2TokenType ID_TOKEN_TOKEN_TYPE = new OAuth2TokenType("id_token");

    protected final OAuth2AuthorizationService authorizationService;
    protected final OAuth2TokenGenerator<? extends OAuth2Token> tokenGenerator;

    @Override
    public Authentication authenticate(Authentication authentication) {
        OAuth2ClientAuthenticationToken clientAuth = (OAuth2ClientAuthenticationToken) authentication.getPrincipal();
        RegisteredClient registeredClient = clientAuth.getRegisteredClient();

        // 소셜 액세스 토큰 추출
        String socialAccessToken = extractAccessToken(authentication);

        // 소셜 유저 정보 요청
        T userInfo = getUserInfo(socialAccessToken);

        // 도메인 유저 조회 (또는 생성)
        User user = getOrLoadUser(userInfo);
        SecurityUser securityUser = SecurityUser.from(user);

        Authentication principalAuth = new UsernamePasswordAuthenticationToken(
                securityUser, null,
                List.of(new SimpleGrantedAuthority(user.getRole().name())));

        DefaultOAuth2TokenContext.Builder ctxBuilder = DefaultOAuth2TokenContext.builder()
                .registeredClient(registeredClient)
                .principal(principalAuth)
                .authorizationServerContext(AuthorizationServerContextHolder.getContext())
                .authorizationGrantType(getGrantTokenType())
                .authorizationGrant(authentication)
                .authorizedScopes(registeredClient.getScopes());

        // 액세스 토큰 생성
        OAuth2Token rawAccess = tokenGenerator.generate(ctxBuilder.tokenType(OAuth2TokenType.ACCESS_TOKEN).build());
        OAuth2AccessToken accessToken = (rawAccess instanceof OAuth2AccessToken at)
                ? at
                : new OAuth2AccessToken(OAuth2AccessToken.TokenType.BEARER, rawAccess.getTokenValue(), rawAccess.getIssuedAt(), rawAccess.getExpiresAt());

        OAuth2RefreshToken refreshToken = null;
        if (registeredClient.getAuthorizationGrantTypes().contains(AuthorizationGrantType.REFRESH_TOKEN)) {
            OAuth2Token rawRefresh = tokenGenerator.generate(ctxBuilder.tokenType(OAuth2TokenType.REFRESH_TOKEN).build());
            if (rawRefresh instanceof OAuth2RefreshToken rt) {
                refreshToken = rt;
            } else {
                throw new OAuth2AuthenticationException(
                        new OAuth2Error(OAuth2ErrorCodes.SERVER_ERROR, "Invalid refresh token", null));
            }
        }

        OidcIdToken idToken;
        if (registeredClient.getScopes().contains(OidcScopes.OPENID)) {
            OAuth2Token rawId = tokenGenerator.generate(ctxBuilder.tokenType(ID_TOKEN_TOKEN_TYPE).build());
            if (rawId instanceof Jwt jwt) {
                idToken = new OidcIdToken(jwt.getTokenValue(), jwt.getIssuedAt(), jwt.getExpiresAt(), jwt.getClaims());
            } else {
                idToken = null;
                throw new OAuth2AuthenticationException(
                        new OAuth2Error(OAuth2ErrorCodes.SERVER_ERROR, "Failed to generate ID token", null));
            }
        } else {
            idToken = null;
        }

        OAuth2Authorization.Builder authzBuilder = OAuth2Authorization
                .withRegisteredClient(registeredClient)
                .principalName(securityUser.getUsername())
                .attribute(Principal.class.getName(), principalAuth)
                .authorizationGrantType(getGrantTokenType())
                .authorizedScopes(registeredClient.getScopes());

        authzBuilder.token(accessToken, meta -> {
            if (rawAccess instanceof Jwt jwt) {
                meta.put(Token.CLAIMS_METADATA_NAME, new java.util.LinkedHashMap<>(jwt.getClaims()));
                meta.put(OAuth2TokenFormat.class.getName(), OAuth2TokenFormat.SELF_CONTAINED.getValue());
            }
        });

        if (refreshToken != null) {
            authzBuilder.refreshToken(refreshToken);
        }

        if (idToken != null) {
            authzBuilder.token(idToken,
                    meta -> meta.put(Token.CLAIMS_METADATA_NAME, idToken.getClaims()));
        }

        authorizationService.save(authzBuilder.build());

        Map<String, Object> additional = (idToken == null)
                ? Collections.emptyMap()
                : Map.of("id_token", idToken.getTokenValue());

        return new OAuth2AccessTokenAuthenticationToken(
                registeredClient, principalAuth, accessToken, refreshToken, additional);
    }

    @Override
    public boolean supports(Class<?> authentication) {
        return getAuthenticationClass().isAssignableFrom(authentication);
    }


    /**
     * 요청에서 소셜 accessToken 추출
     */
    protected abstract String extractAccessToken(Authentication authentication);

    /**
     * 소셜 유저 정보 요청
     */
    protected abstract T getUserInfo(String accessToken);

    /**
     * 해당 유저를 서비스 DB에서 조회하거나 생성
     */
    protected abstract User getOrLoadUser(T userInfo);

    /**
     * 커스텀 GrantType 리턴
     */
    protected abstract AuthorizationGrantType getGrantTokenType();

    /**
     * 해당 AuthenticationToken 클래스 리턴
     */
    protected abstract Class<? extends Authentication> getAuthenticationClass();
}