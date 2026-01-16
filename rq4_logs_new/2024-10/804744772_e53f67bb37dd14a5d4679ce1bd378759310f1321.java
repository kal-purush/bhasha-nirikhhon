package com.example.controller;


import com.example.model.Component;
import com.example.model.Electronics;
import com.example.repository.ElectronicsRepository;
import com.example.service.ElectronicsService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/electronics")
public class ElectronicsController {
    private final ElectronicsService electronicsService;
    private final ElectronicsRepository electronicsRepository;

    public ElectronicsController(ElectronicsService electronicsService, ElectronicsRepository electronicsRepository) {
        this.electronicsService = electronicsService;
        this.electronicsRepository = electronicsRepository;
    }
    @GetMapping("/{id}")
    public ResponseEntity<Electronics> getElectronicsById(@PathVariable int id){
        Electronics electronics =  electronicsService.getElectronicsById((id));
        if (electronics != null){
            return ResponseEntity.ok(electronics);
        }else {
            return ResponseEntity.notFound().build();
        }
    }
    @PostMapping("/create")
    public ResponseEntity<Electronics> createElectronics(@RequestBody Electronics electronics){
        Electronics createdElectronics = electronicsService.createElectronics(electronics);
        return ResponseEntity.ok(createdElectronics);
    }
    @PutMapping("/{id}")
    public ResponseEntity<Electronics> updateElectronics(@PathVariable int id, @RequestBody Electronics electronics){
        Electronics existingElectronics = electronicsRepository.findById((long) id).orElse(null);
        if (existingElectronics != null){
            existingElectronics.setBrand(electronics.getBrand());
            existingElectronics.setModel(electronics.getModel());
            existingElectronics.setPrice(electronics.getPrice());
            Electronics updatedElectronics = electronicsRepository.save(existingElectronics);
            return ResponseEntity.ok(updatedElectronics);
        }else {
            return ResponseEntity.notFound().build();
        }
    }
    @DeleteMapping("/{id}")
    public ResponseEntity<Void> deleteElectronics(@PathVariable int id){
        boolean isDeleted = electronicsService.deleteElectronics(id);
        electronicsRepository.deleteById((long) id);
        return ResponseEntity.noContent().build();
    }
    @PatchMapping("/{id}/components")
    public ResponseEntity<Electronics> addComponentToElectronics(
            @PathVariable int id,
            @RequestBody List<Component> newComponents) {

        Electronics existingElectronics = electronicsRepository.findById((long) id).orElse(null);
        if (existingElectronics == null) {
            return ResponseEntity.notFound().build();
        }

        List<Component> existingComponents = existingElectronics.getComponents();
        existingComponents.addAll(newComponents);
        existingElectronics.setComponents(existingComponents);
        Electronics updatedElectronics = electronicsRepository.save(existingElectronics);
        return ResponseEntity.ok(updatedElectronics);
    }
}