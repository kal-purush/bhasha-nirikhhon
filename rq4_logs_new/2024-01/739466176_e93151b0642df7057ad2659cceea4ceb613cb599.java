package com.anghel.music_band_organizer.controllers;

import com.anghel.music_band_organizer.models.dtos.BandDTO;
import com.anghel.music_band_organizer.services.band.BandService;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/bands")
public class BandController {

    private final BandService bandService;

    public BandController(BandService bandService) {
        this.bandService = bandService;
    }

    @PostMapping
    public ResponseEntity<BandDTO> createBand(@Valid @RequestBody BandDTO bandDTO) {
        return ResponseEntity.ok(bandService.createBand(bandDTO));
    }

    @GetMapping
    public ResponseEntity<List<BandDTO>> getBands() {
        return ResponseEntity.ok(bandService.getAllBands());
    }

    @GetMapping("/{bandId}")
    public ResponseEntity<BandDTO> getBandById(@PathVariable Long bandId) {
        return ResponseEntity.ok(bandService.getBandById(bandId));
    }

    @DeleteMapping("/{bandId}")
    public ResponseEntity<String> deleteBandById(@PathVariable Long bandId) {
        return ResponseEntity.ok(bandService.deleteBandById(bandId));
    }
}