package no.fint;

import no.fint.oauth.OAuthConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;
import org.springframework.hateoas.config.EnableHypermediaSupport;
import org.springframework.scheduling.annotation.EnableScheduling;

import static org.springframework.hateoas.config.EnableHypermediaSupport.HypermediaType.HAL;

@EnableScheduling
@Import(OAuthConfig.class)
@EnableHypermediaSupport(type = HAL)
@SpringBootApplication
public class Application {

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

}