package com.example.backend.controllers;

import com.example.backend.dtos.AsteroidOutgoingDTO;
import com.example.backend.services.AsteroidService;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;


@RestController
@RequestMapping("/api/asteroid")
@CrossOrigin
public class AsteroidController {

    private final Logger logger = LoggerFactory.getLogger(AsteroidController.class);

    AsteroidService asteroidService;

    public AsteroidController(AsteroidService asteroidService) {
        this.asteroidService = asteroidService;
    }

    @GetMapping("/{width}/{height}")
    public ResponseEntity<AsteroidOutgoingDTO> getAsteroidData(@PathVariable int width, @PathVariable int height) {
        AsteroidOutgoingDTO asteroidOutgoingDTO = asteroidService.getAsteroidData(width, height);
        if(asteroidOutgoingDTO.getError() == null) {
            return ResponseEntity.ok(asteroidOutgoingDTO);
        }
        return ResponseEntity.badRequest().body(asteroidOutgoingDTO);
    }
}