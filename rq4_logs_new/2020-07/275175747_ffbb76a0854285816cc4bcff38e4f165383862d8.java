package jm;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.ViewControllerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
public class WebConfig implements WebMvcConfigurer {
    @Override
    public void addViewControllers(ViewControllerRegistry registry) {
        registry.addViewController("/login").setViewName("login");
        registry.addViewController("/").setViewName("index");
        registry.addViewController("/news").setViewName("news");
        registry.addViewController("/text-me-the-app").setViewName("text-me-the-app");
        registry.addViewController("/portfolios").setViewName("portfolios");
        registry.addViewController("/how-it-works").setViewName("how-it-works");
        registry.addViewController("/help").setViewName("help");
        registry.addViewController("/sell").setViewName("sell");
        registry.addViewController("/reviews").setViewName("reviews");
        registry.addViewController("/privacy").setViewName("privacy");
        registry.addViewController("/terms").setViewName("terms");
        registry.addViewController("/jobs").setViewName("jobs");
        registry.addViewController("/press").setViewName("press");
    }

    @Override
    public void addCorsMappings(CorsRegistry registry) {
        registry.addMapping("/**")
                .allowedOrigins("http://localhost:8888")
                .allowedMethods("*");
    }
}