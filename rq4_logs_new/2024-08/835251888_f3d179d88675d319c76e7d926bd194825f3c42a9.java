package dev.forkingaround.zootopia.controllers;

import dev.forkingaround.zootopia.models.Animal;
import dev.forkingaround.zootopia.repositories.AnimalRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Optional;

@RestController
@RequestMapping("/api/animals")
public class AnimalController {

    @Autowired
    private AnimalRepository animalRepository;

    @GetMapping
    public ResponseEntity<List<Animal>> getAllAnimals() {
        List<Animal> animals = animalRepository.findAll();
        return ResponseEntity.ok(animals);
    }

    @GetMapping("/{id}")
    public ResponseEntity<Animal> getAnimalById(@PathVariable Long id) throws Exception {
        Optional<Animal> animal = animalRepository.findById(id);
        if (animal.isPresent()) {
            return ResponseEntity.ok(animal.get());
        } else {
            throw new Exception("Animal not found with ID " + id);
        }
    }

    @PostMapping("/add")
    public ResponseEntity<Animal> createAnimal(@RequestBody Animal newAnimal) {
        Animal createdAnimal = animalRepository.save(newAnimal);
        return ResponseEntity.status(HttpStatus.CREATED).body(createdAnimal);
    }

    @PutMapping("/{id}")
    public ResponseEntity<Animal> updateAnimal(@PathVariable Long id, @RequestBody Animal updatedAnimal) throws Exception {
        if (!animalRepository.existsById(id)) {
            throw new Exception("Animal not found with ID " + id);
        }
        updatedAnimal.setId(id);
        Animal animal = animalRepository.save(updatedAnimal);
        return ResponseEntity.ok(animal);
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> deleteAnimal(@PathVariable Long id) throws Exception {
        if (!animalRepository.existsById(id)) {
            throw new Exception("Animal not found with ID " + id);
        }
        animalRepository.deleteById(id);
        return ResponseEntity.noContent().build();
    }
}