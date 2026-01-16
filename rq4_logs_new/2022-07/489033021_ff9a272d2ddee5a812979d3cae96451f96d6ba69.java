package org.ujar.loremipsum.shared.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(ProjectInfoProperties.class)
public class ProjectInfoConfig {
}