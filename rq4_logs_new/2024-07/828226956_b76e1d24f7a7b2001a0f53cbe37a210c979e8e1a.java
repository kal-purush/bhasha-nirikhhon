package adhd.diary.global.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
@EnableWebMvc
public class WebMvcConfig implements WebMvcConfigurer {

    @Value("${spring.cors.allowed-origins}")
    private String allowedOrigins;

    @Value("${spring.cors.allowed-methods}")
    private String allowedMethods;

    @Value("${spring.cors.allowed-headers}")
    private String allowedHeaders;

    @Value("${spring.cors.allow-credentials}")
    private boolean allowCredentials;

    @Override
    public void addCorsMappings(CorsRegistry registry) {
        registry.addMapping("/**")
                .allowedHeaders(allowedHeaders)
                .allowedMethods(allowedMethods.split(","))
                .allowedOrigins(allowedOrigins)
                .allowCredentials(allowCredentials)
                .maxAge(3600);
    }
}