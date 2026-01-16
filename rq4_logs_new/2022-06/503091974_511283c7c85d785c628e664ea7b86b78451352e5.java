package org.fabm.backend.profile.configuration;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
public class ResourceConfig implements WebMvcConfigurer {

    @Override
    public void addResourceHandlers(ResourceHandlerRegistry registry) {
        String audioPath = "file:///C:/Users/955444/audios/";
        String photoPath = "file:///C:/Users/955444/photos/";
        registry.addResourceHandler("/content/**")
                .addResourceLocations(audioPath)
                .addResourceLocations(photoPath);
    }
}