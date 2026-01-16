package io.github.yoshikawaa.sample;

import java.net.URI;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.security.oauth2.client.EnableOAuth2Sso;
import org.springframework.context.annotation.Bean;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.oauth2.client.OAuth2ClientContext;
import org.springframework.security.oauth2.client.OAuth2RestTemplate;
import org.springframework.security.oauth2.client.resource.OAuth2ProtectedResourceDetails;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.util.UriComponentsBuilder;

import io.github.yoshikawaa.sample.entity.Repository;

@SpringBootApplication
@EnableWebSecurity
@EnableOAuth2Sso
@Controller
public class Application {

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    @Bean
    public OAuth2RestTemplate oauth2RestTemplate(OAuth2ClientContext oauth2ClientContext,
            OAuth2ProtectedResourceDetails details) {
        return new OAuth2RestTemplate(details, oauth2ClientContext);
    }

    @GetMapping("/")
    public String home() {
        return "home";
    }

    @GetMapping("/oauth")
    public String oauth() {
        return "oauth";
    }

    @Autowired
    private OAuth2RestTemplate auth2RestTemplate;

    @GetMapping("/repos")
    public String repositories(Model model) {
        URI uri = UriComponentsBuilder.fromUriString("https://api.github.com/user/repos").build().toUri();
        model.addAttribute("repos", auth2RestTemplate.getForEntity(uri, Repository[].class).getBody());
        return "repos";
    }
}