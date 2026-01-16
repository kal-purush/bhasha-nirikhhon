package moofarm.ssuckssuck.global.oauth.google;

import lombok.extern.slf4j.Slf4j;
import moofarm.ssuckssuck.domain.oauth.client.OauthMemberClient;
import moofarm.ssuckssuck.domain.oauth.domain.OauthMember;
import moofarm.ssuckssuck.domain.oauth.domain.OauthServerType;
import moofarm.ssuckssuck.global.oauth.google.client.GoogleApiClient;
import moofarm.ssuckssuck.global.oauth.google.dto.GoogleMemberResponse;
import moofarm.ssuckssuck.global.oauth.google.dto.GoogleToken;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

@Slf4j
@Component
@RequiredArgsConstructor
public class GoogleMemberClient implements OauthMemberClient {

    private final GoogleApiClient googleApiClient;
    private final GoogleOauthConfig googleOauthConfig;

    @Override
    public OauthServerType supportServer() {
        return OauthServerType.GOOGLE;
    }

    @Override
    public OauthMember fetch(String authCode) {
        String code = URLDecoder.decode(authCode, StandardCharsets.UTF_8);
        GoogleToken tokenInfo = googleApiClient.fetchToken(tokenRequestParams(code));
        log.info(tokenInfo.toString());
        GoogleMemberResponse googleMemberResponse = googleApiClient.fetchMember("Bearer " + tokenInfo.accessToken());
        log.info(googleMemberResponse.toString());
        return googleMemberResponse.toDomain();
    }

    private MultiValueMap<String, String> tokenRequestParams(String authCode) {
        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        params.add("code", authCode);
        params.add("client_id", googleOauthConfig.clientId());
        params.add("client_secret", googleOauthConfig.clientSecret());
        params.add("redirect_uri", googleOauthConfig.redirectUri());
        params.add("grant_type", "authorization_code");
        log.info(params.toString());
        return params;
    }
}