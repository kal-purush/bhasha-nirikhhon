package com.thanksd.server.security.auth.google;

import com.thanksd.server.exception.unauthorized.InvalidGoogleTokenException;
import com.thanksd.server.security.auth.OAuthPlatformMemberResponse;
import feign.FeignException;
import org.springframework.stereotype.Component;

@Component
public class GoogleOAuthUserProvider {

    private final GoogleAuthClient googleAuthClient;
    private final AuthProperties authProperties;
    private final GoogleInfoClient googleInfoClient;

    public GoogleOAuthUserProvider(GoogleAuthClient googleAuthClient, AuthProperties authProperties, GoogleInfoClient googleInfoClient) {
        this.googleAuthClient = googleAuthClient;
        this.authProperties = authProperties;
        this.googleInfoClient = googleInfoClient;
    }

    public OAuthPlatformMemberResponse getGooglePlatformMember(String code) {
        try {
            String googleToken = googleAuthClient.getGoogleToken(
                    createRequest(code)
            ).getAccessToken();
            GoogleInfoResponse user = googleInfoClient.getUserInfo(googleToken);
            return new OAuthPlatformMemberResponse(user.getEmail(), user.getEmail());
        } catch (FeignException e) {
            throw new InvalidGoogleTokenException();
        }
    }

    public GoogleTokenRequest createRequest(String code) {
        return GoogleTokenRequest.builder()
                .code(code)
                .clientId(authProperties.getClientId())
                .clientSecret(authProperties.getClientSecret())
                .redirectUri(authProperties.getRedirectUri())
                .build();
    }
}