package leets.weeth.global.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Getter
@Setter
@Component
@ConfigurationProperties(prefix = "auth")
public class OauthProviderProperties {
    private Map<String, Provider> providers = new HashMap<>();

    @Getter @Setter
    public static class Provider {
        private String authorizeUri;
        private String clientId;
        private String redirectUri;
        private String grantType;
    }
}