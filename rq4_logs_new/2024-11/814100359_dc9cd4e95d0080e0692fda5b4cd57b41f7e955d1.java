package soma.edupiuser.web.client;

import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.service.annotation.GetExchange;
import org.springframework.web.service.annotation.HttpExchange;

@Component
@HttpExchange
public interface NaverOAuth2ApiClient {

    @GetExchange("/oauth2.0/token")
    void unlink(@RequestParam("service_provider") String serviceProvider,
        @RequestParam("grant_type") String grantType,
        @RequestParam("client_id") String clientId,
        @RequestParam("client_secret") String clientSecret,
        @RequestParam("access_token") String accessToken);
}