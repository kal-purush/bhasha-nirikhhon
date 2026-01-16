package moofarm.ssuckssuck.global.oauth.naver.authcode;

import lombok.RequiredArgsConstructor;
import moofarm.ssuckssuck.domain.oauth.authcode.AuthCodeRequestUrlProvider;
import moofarm.ssuckssuck.domain.oauth.domain.OauthServerType;
import moofarm.ssuckssuck.global.oauth.naver.NaverOauthConfig;
import org.springframework.stereotype.Component;
import org.springframework.web.util.UriComponentsBuilder;

@Component
@RequiredArgsConstructor
public class NaverAuthCodeRequestUrlProvider implements AuthCodeRequestUrlProvider {
    private final NaverOauthConfig naverOauthConfig;

    @Override
    public OauthServerType supportServer() {
        return OauthServerType.NAVER;
    }

    @Override
    public String provide() {
        return UriComponentsBuilder
                .fromUriString("https://nid.naver.com/oauth2.0/authorize\t")
                .queryParam("response_type", " code")
                .queryParam("client_id", naverOauthConfig.clientId())
                .queryParam("redirect_uri", naverOauthConfig.redirectUri())
                .queryParam("state", naverOauthConfig.state())
                .toUriString();
    }
}