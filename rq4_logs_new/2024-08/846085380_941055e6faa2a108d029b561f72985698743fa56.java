package vn.hust.hedspi.ezsport.controllers;

import jakarta.validation.Valid;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import vn.hust.hedspi.ezsport.dtos.sport.CreateSportRequest;
import vn.hust.hedspi.ezsport.dtos.sport.UpdateSportRequest;
import vn.hust.hedspi.ezsport.entities.Sport;
import vn.hust.hedspi.ezsport.repositories.SportRepository;

import java.util.List;
import java.util.UUID;

@RestController
@NoArgsConstructor
@AllArgsConstructor
@RequestMapping("api/v1/sport")
public class SportController {
    @Autowired
    private SportRepository sportRepository;

    // Create
    @PostMapping()
    public ResponseEntity<Sport> createSport(@Valid @RequestBody CreateSportRequest requestBody) {
        Sport sport = new Sport();
        sport.setName(requestBody.getName());

        return ResponseEntity.ok(sportRepository.save(sport));
    }

    // List Sport
    @GetMapping()
    public ResponseEntity<List<Sport>> getAllSports() {
        List<Sport> sports = sportRepository.findAll();

        return ResponseEntity.ok(sports);
    }

    // Show
    @GetMapping("/{id}")
    public ResponseEntity<Sport> getSportById(@PathVariable UUID id) {
        Sport sport = sportRepository.findById(id).orElse(null);

        return sport != null ? ResponseEntity.ok(sport) : ResponseEntity.notFound().build();
    }

    // Update
    @PutMapping("/{id}")
    public ResponseEntity<Sport> updateSport(@PathVariable UUID id, @Valid @RequestBody UpdateSportRequest requestBody) {
        Sport sport = sportRepository.findById(id).orElse(null);

        if (sport == null) {
            return ResponseEntity.notFound().build();
        }

        sport.setName(requestBody.getName());

        return ResponseEntity.ok(sportRepository.save(sport));
    }

    // Delete
    @DeleteMapping("/{id}")
    public ResponseEntity<Void> deleteSport(@PathVariable UUID id) {
        Sport sport = sportRepository.findById(id).orElse(null);

        if (sport == null) {
            return ResponseEntity.notFound().build();
        }

        sportRepository.delete(sport);

        return ResponseEntity.noContent().build();
    }
}