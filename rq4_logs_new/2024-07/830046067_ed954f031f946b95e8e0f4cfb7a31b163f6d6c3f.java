package com.example.librarymanagement.configuration;

import com.example.librarymanagement.converter.StringToEnumConverter;
import org.springframework.context.annotation.Configuration;
import org.springframework.format.FormatterRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
public class WebConfiguration implements WebMvcConfigurer {

    @Override
    public void addFormatters(FormatterRegistry registry) {
        registry.addConverter(new StringToEnumConverter.StringToEnumGenreConverter());
        registry.addConverter(new StringToEnumConverter.StringToEnumAvailabilityConverter());
    }
}