package org.apilytic.currency.persistence;

import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.repository.configuration.EnableRedisRepositories;

@Configuration
@EnableRedisRepositories(repositoryImplementationPostfix = "Service")
public class ApplicationConfiguration {
}