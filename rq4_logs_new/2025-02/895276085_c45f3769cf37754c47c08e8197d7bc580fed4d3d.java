package com.spotifyapi.controller;

import com.spotifyapi.service.SpotifyReleaseService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/test")
@RequiredArgsConstructor
@Slf4j
public class RabbitController {

    private final SpotifyReleaseService spotifyReleaseService;

    @GetMapping("/queue")
    public void testRabbit() {
        spotifyReleaseService.checkReleasesForAllUsers();
        log.info("Controller to test RABBIT");

    }

}