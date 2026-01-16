package com.example.ais_v5.controllers;

import com.example.ais_v5.entity.Groupe;
import com.example.ais_v5.services.GroupeService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/v1/groupes")
@RequiredArgsConstructor
public class GroupeController {
    private final GroupeService groupeService;

    @GetMapping
    public List<Groupe> getAllGroupes() {
        return groupeService.getAllGroupes();
    }

    @GetMapping("/{id}")
    public ResponseEntity<Groupe> getGroupeById(@PathVariable Long id) {
        return ResponseEntity.ok(groupeService.getGroupeById(id));
    }

    @PostMapping
    public ResponseEntity<Groupe> createGroupe(@RequestBody Groupe groupe) {
        return ResponseEntity.status(HttpStatus.CREATED).body(groupeService.createGroupe(groupe));
    }

    @PutMapping("/{id}")
    public ResponseEntity<Groupe> updateGroupe(@PathVariable Long id, @RequestBody Groupe groupe) {
        return ResponseEntity.ok(groupeService.updateGroupe(id, groupe));
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> deleteGroupe(@PathVariable Long id) {
        groupeService.deleteGroupe(id);
        return ResponseEntity.noContent().build();
    }
}