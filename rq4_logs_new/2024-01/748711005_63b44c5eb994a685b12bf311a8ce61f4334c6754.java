package com.example.demo.config.property;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.List;

/**
 * @author Sattya
 * create at 1/29/2024 4:37 PM
 */
@Configuration
@ConfigurationProperties(prefix = "spring.security")
@Data
public class SecurityProperties {
    private String allowOrigin;
    private List<String> allowedHeader;
    private List<String> allowedMethod;
}