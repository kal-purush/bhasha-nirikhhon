package com.anghel.music_band_organizer.controllers;

import com.anghel.music_band_organizer.models.dtos.RehearsalDTO;
import com.anghel.music_band_organizer.services.rehearsal.RehearsalService;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/rehearsals")
public class RehearsalController {

    private final RehearsalService rehearsalService;

    public RehearsalController(RehearsalService rehearsalService) {
        this.rehearsalService = rehearsalService;
    }

    @PostMapping("/{bandId}")
    public ResponseEntity<RehearsalDTO> createRehearsal(@Valid @RequestBody RehearsalDTO rehearsalDTO, @PathVariable Long bandId) {
        return ResponseEntity.ok(rehearsalService.createRehearsal(rehearsalDTO, bandId));
    }
}