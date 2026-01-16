package com.example.swiftgathering_server.config;

import lombok.Getter;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import java.time.Instant;

@Component
public class ApplicationStartup {
    @Getter
    private static Instant startTime;

    @PostConstruct
    public void onStartup() {
        startTime = Instant.now();
    }
}