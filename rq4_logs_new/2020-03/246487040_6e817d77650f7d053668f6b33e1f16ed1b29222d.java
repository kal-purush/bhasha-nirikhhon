package org.mycode.controller;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

@Component("healthCheck")
public class HealthCheckInDetails implements HealthIndicator {
    @Value("${spring.application.name}")
    private String appName;

    @Override
    public Health health() {
        return Health.up().withDetail("appName", appName).build();
    }
}