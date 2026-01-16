package soma.edupiuser.oauth.service;

import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import soma.edupiuser.account.models.EmailRequest;
import soma.edupiuser.account.service.domain.Account;
import soma.edupiuser.oauth.models.OAuth2Provider;
import soma.edupiuser.oauth.models.OAuth2UserUnlinkManager;
import soma.edupiuser.oauth.models.SignupOauthRequest;
import soma.edupiuser.web.auth.TokenProvider;
import soma.edupiuser.web.client.MetaServerApiClient;

@Service
@Slf4j
@RequiredArgsConstructor
public class OAuth2AccountService {

    private final MetaServerApiClient metaServerApiClient;
    private final TokenProvider tokenProvider;
    private final OAuth2UserUnlinkManager oAuth2UserUnlinkManager;


    public void handleLogin(OAuth2UserPrincipal principal, HttpServletResponse response) {
        String email = principal.getUserInfo().getEmail();
        String name = principal.getUserInfo().getName();

        if (!metaServerApiClient.isExistsEmail(email)) {
            log.info("handleLogin - signup, email={}", email);
            metaServerApiClient.saveAccountWithOauth(SignupOauthRequest.builder()
                .email(email)
                .name(name)
                .build());
        }

        log.info("handleLogin - login, email={}", email);
        Account account = metaServerApiClient.login(new EmailRequest(email));
        String accessToken = tokenProvider.generateToken(account);
        addAccessTokenToCookie(response, accessToken);
    }

    public void handleUnlink(OAuth2UserPrincipal principal) {
        log.info("handleUnlink");
        String accessToken = principal.getUserInfo().getAccessToken();
        OAuth2Provider provider = principal.getUserInfo().getProvider();
        oAuth2UserUnlinkManager.unlink(provider, accessToken);
    }

    private void addAccessTokenToCookie(HttpServletResponse response, String accessToken) {
        Cookie cookie = new Cookie("token", accessToken);
        cookie.setHttpOnly(true);
        cookie.setPath("/");
        cookie.setMaxAge(60 * 60);
        response.addCookie(cookie);
    }
}