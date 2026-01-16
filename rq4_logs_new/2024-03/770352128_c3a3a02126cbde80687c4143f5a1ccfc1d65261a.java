package moofarm.ssuckssuck.global.oauth.google.authcode;

import lombok.RequiredArgsConstructor;
import moofarm.ssuckssuck.domain.oauth.authcode.AuthCodeRequestUrlProvider;
import moofarm.ssuckssuck.domain.oauth.domain.OauthServerType;
import moofarm.ssuckssuck.global.oauth.google.GoogleOauthConfig;
import org.springframework.stereotype.Component;
import org.springframework.web.util.UriComponentsBuilder;

@Component
@RequiredArgsConstructor
public class GoogleAuthCodeRequestUrlProvider implements AuthCodeRequestUrlProvider {

    private final GoogleOauthConfig googleOauthConfig;

    @Override
    public OauthServerType supportServer() {
        return OauthServerType.GOOGLE;
    }

    @Override
    public String provide() {
        return UriComponentsBuilder
                .fromUriString("https://accounts.google.com/o/oauth2/v2/auth")
                .queryParam("client_id", googleOauthConfig.clientId())
                .queryParam("redirect_uri", googleOauthConfig.redirectUri())
                .queryParam("response_type", "code")
                .queryParam("scope", googleOauthConfig.scope())
                .build()
                .toUriString();
    }
}