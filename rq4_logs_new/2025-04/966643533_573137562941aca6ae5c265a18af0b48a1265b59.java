package org.spiceboys.Travel.Diary.controller;

import org.spiceboys.Travel.Diary.model.Itinerary;
import org.spiceboys.Travel.Diary.service.ItineraryService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api")
public class ItineraryController {
    private final ItineraryService itineraryService;

    public ItineraryController(ItineraryService itineraryService) {
        this.itineraryService = itineraryService;
    }

    @PostMapping("/itineraries")
    public ResponseEntity<Itinerary> createItinerary(@RequestBody Itinerary itinerary) {
        Itinerary createdItinerary = itineraryService.createItinerary(itinerary);
        return ResponseEntity.status(HttpStatus.CREATED).body(createdItinerary);
    }

    @GetMapping("/users/{username}/itineraries")
    public ResponseEntity<List<Itinerary>> getItinerariesByUsername(@PathVariable String username) {
        List<Itinerary> itineraries = itineraryService.getItinerariesByUsername(username);
        return ResponseEntity.status(HttpStatus.OK).body(itineraries);
    }
}




