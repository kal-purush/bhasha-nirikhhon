package example.util;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestTemplate;

@Configuration
public class Config {

    /*Injetando RestTemplate como Bean*/
    @Bean
    public RestTemplate getRestTemplate() {
        return new RestTemplate();
    }
}