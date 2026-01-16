package com.hillel.multi.actuator;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.Map;

import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

@Component
@Endpoint(id = "custom-health-check-endpoint")
public class CustomHealthCheckEndpointIndicator implements HealthIndicator {

    private final String message_key = "Custom health check endpoint";
    @ReadOperation
    public Map<String, String> getStatus() {
        Map<String, String> map = new HashMap<>();
        map.put("server.status", "ok");
        map.put("server.date", LocalDate.now().toString());
        map.put("server.time", LocalTime.now().toString());
        return map;
    }

    @Override
    public Health health() {
        if (!isRunningCustomHealthCheckEndpoint()) {
            return Health.down().withDetail(message_key, "Not Available").build();
        }
        return Health.up().withDetail(message_key, "Available").build();
    }

    private Boolean isRunningCustomHealthCheckEndpoint() {
        return !getStatus().isEmpty();
    }
}