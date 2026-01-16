package com.bahubba.bahubbabookclub.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

import java.time.Duration;

@RestController
@RequestMapping("/api/v1/notification")
@RequiredArgsConstructor
public class NotificationController {
    @GetMapping("/poc")
    public Flux<Long> poc() {
        return Flux.interval(Duration.ofMinutes(5))
            .map(i -> i);
    }
}