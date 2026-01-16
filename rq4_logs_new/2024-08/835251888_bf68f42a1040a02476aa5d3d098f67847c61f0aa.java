package dev.forkingaround.zootopia.controllers;

import dev.forkingaround.zootopia.models.Animal;
import dev.forkingaround.zootopia.models.Family;
import dev.forkingaround.zootopia.models.Type;
import dev.forkingaround.zootopia.repositories.AnimalRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

public class AnimalControllerTest {

    @Mock
    private AnimalRepository animalRepository;

    @InjectMocks
    private AnimalController animalController;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void testGetAllAnimals() {

        Family mammalFamily = new Family();
        mammalFamily.setId(1L);
        mammalFamily.setName("Mammals");

        Type mammalType = new Type(1L, "Mammal", mammalFamily, new HashSet<>());
        List<Animal> animals = Arrays.asList(
                new Animal(1L, "Lion", "Male", new Date(), mammalType),
                new Animal(2L, "Elephant", "Female", new Date(), mammalType));
        when(animalRepository.findAll()).thenReturn(animals);

        ResponseEntity<List<Animal>> response = animalController.getAllAnimals();

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(animals.size(), response.getBody().size());
        verify(animalRepository, times(1)).findAll();
    }

    @Test
    void testGetAnimalById_Found() {

        Family mammalFamily = new Family();
        mammalFamily.setId(1L);
        mammalFamily.setName("Mammals");

        Type mammalType = new Type(1L, "Mammal", mammalFamily, new HashSet<>());
        Animal animal = new Animal(1L, "Lion", "Male", new Date(), mammalType);
        when(animalRepository.findById(1L)).thenReturn(Optional.of(animal));

        ResponseEntity<Animal> response = animalController.getAnimalById(1L);

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(animal, response.getBody());
        verify(animalRepository, times(1)).findById(1L);
    }

    @Test
    void testGetAnimalById_NotFound() {

        when(animalRepository.findById(1L)).thenReturn(Optional.empty());

        ResponseEntity<Animal> response = animalController.getAnimalById(1L);

        assertEquals(HttpStatus.NOT_FOUND, response.getStatusCode());
        assertEquals(null, response.getBody());
        verify(animalRepository, times(1)).findById(1L);
    }

    @Test
    void testCreateAnimal() {

        Family mammalFamily = new Family();
        mammalFamily.setId(1L);
        mammalFamily.setName("Mammals");

        Type mammalType = new Type(1L, "Mammal", mammalFamily, new HashSet<>());
        Animal newAnimal = new Animal(null, "Tiger", "Female", new Date(), mammalType);
        Animal savedAnimal = new Animal(1L, "Tiger", "Female", new Date(), mammalType);
        when(animalRepository.save(newAnimal)).thenReturn(savedAnimal);

        ResponseEntity<Animal> response = animalController.createAnimal(newAnimal);

        assertEquals(HttpStatus.CREATED, response.getStatusCode());
        assertEquals(savedAnimal, response.getBody());
        verify(animalRepository, times(1)).save(newAnimal);
    }

    @Test
    void testUpdateAnimal_Found() {

        Family mammalFamily = new Family();
        mammalFamily.setId(1L);
        mammalFamily.setName("Mammals");

        Type mammalType = new Type(1L, "Mammal", mammalFamily, new HashSet<>());
        Animal updatedAnimal = new Animal(1L, "Tiger", "Female", new Date(), mammalType);
        when(animalRepository.existsById(1L)).thenReturn(true);
        when(animalRepository.save(updatedAnimal)).thenReturn(updatedAnimal);

        ResponseEntity<Animal> response = animalController.updateAnimal(1L, updatedAnimal);

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(updatedAnimal, response.getBody());
        verify(animalRepository, times(1)).existsById(1L);
        verify(animalRepository, times(1)).save(updatedAnimal);
    }

    @Test
    void testUpdateAnimal_NotFound() {

        Family mammalFamily = new Family();
        mammalFamily.setId(1L);
        mammalFamily.setName("Mammals");

        Type mammalType = new Type(1L, "Mammal", mammalFamily, new HashSet<>());
        Animal updatedAnimal = new Animal(1L, "Tiger", "Female", new Date(), mammalType);
        when(animalRepository.existsById(1L)).thenReturn(false);

        ResponseEntity<Animal> response = animalController.updateAnimal(1L, updatedAnimal);

        assertEquals(HttpStatus.NOT_FOUND, response.getStatusCode());
        verify(animalRepository, times(1)).existsById(1L);
        verify(animalRepository, never()).save(updatedAnimal);
    }

    @Test
    void testDeleteAnimal_Found() {

        when(animalRepository.existsById(1L)).thenReturn(true);

        ResponseEntity<Void> response = animalController.deleteAnimal(1L);

        assertEquals(HttpStatus.NO_CONTENT, response.getStatusCode());
        verify(animalRepository, times(1)).existsById(1L);
        verify(animalRepository, times(1)).deleteById(1L);
    }

    @Test
    void testDeleteAnimal_NotFound() {

        when(animalRepository.existsById(1L)).thenReturn(false);

        ResponseEntity<Void> response = animalController.deleteAnimal(1L);

        assertEquals(HttpStatus.NOT_FOUND, response.getStatusCode());
        verify(animalRepository, times(1)).existsById(1L);
        verify(animalRepository, never()).deleteById(1L);
    }
}