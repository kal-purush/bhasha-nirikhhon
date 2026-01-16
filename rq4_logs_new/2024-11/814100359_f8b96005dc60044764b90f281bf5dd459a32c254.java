package soma.edupiuser.web.client.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.support.RestClientAdapter;
import org.springframework.web.service.invoker.HttpServiceProxyFactory;

@Configuration
public class GoogleOAuth2ApiClientConfig {

    private static final String GOOGLE_API_URL = "https://oauth2.googleapis.com";

    @Bean
    public GoogleOAuth2ApiClientConfig googleOAuth2ApiClient() {
        RestClient restClient = RestClient.builder()
            .baseUrl(GOOGLE_API_URL)
            .build();

        RestClientAdapter adapter = RestClientAdapter.create(restClient);
        HttpServiceProxyFactory factory = HttpServiceProxyFactory
            .builderFor(adapter)
            .build();

        return factory.createClient(GoogleOAuth2ApiClientConfig.class);
    }

}