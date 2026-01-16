package dev.forkingaround.zootopia.services;

import dev.forkingaround.zootopia.dtos.AnimalRequest;
import dev.forkingaround.zootopia.models.Animal;
import dev.forkingaround.zootopia.models.Type;
import dev.forkingaround.zootopia.repositories.AnimalRepository;
import dev.forkingaround.zootopia.repositories.TypeRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class AnimalServiceTest {

    @InjectMocks
    private AnimalService animalService;

    @Mock
    private AnimalRepository animalRepository;

    @Mock
    private TypeRepository typeRepository;

    private Date parseDate(String dateStr) throws ParseException {
        return new SimpleDateFormat("yyyy-MM-dd").parse(dateStr);
    }

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void testGetAllAnimals() {
        List<Animal> animals = new ArrayList<>();
        animals.add(new Animal());
        when(animalRepository.findAll()).thenReturn(animals);
        List<Animal> result = animalService.getAllAnimals();
        assertEquals(animals, result);
    }

    @Test
    void testGetAnimalByIdFound() {
        Animal animal = new Animal();
        when(animalRepository.findById(anyLong())).thenReturn(Optional.of(animal));
        Optional<Animal> result = animalService.getAnimalById(1L);
        assertEquals(Optional.of(animal), result);
    }

    @Test
    void testGetAnimalByIdNotFound() {
        when(animalRepository.findById(anyLong())).thenReturn(Optional.empty());
        Optional<Animal> result = animalService.getAnimalById(1L);
        assertEquals(Optional.empty(), result);
    }

    @Test
    void testCreateAnimalSuccess() throws ParseException {
        AnimalRequest request = new AnimalRequest();
        request.setName("Lion");
        request.setTypeName("Wild");
        request.setGender("Male");
        request.setDateOfEntry(parseDate("2024-01-01"));

        Type type = new Type();
        when(typeRepository.findByName(anyString())).thenReturn(Optional.of(type));

        Animal animal = Animal.builder()
                .name("Lion")
                .type(type)
                .gender("Male")
                .dateOfEntry(parseDate("2024-01-01"))
                .build();
        when(animalRepository.save(any(Animal.class))).thenReturn(animal);
        Animal result = animalService.createAnimal(request);
        assertEquals(animal, result);
    }

    @Test
    void testCreateAnimalTypeNotFound() {
        AnimalRequest request = new AnimalRequest();
        request.setTypeName("Unknown");
        when(typeRepository.findByName(anyString())).thenReturn(Optional.empty());
        assertThrows(IllegalArgumentException.class, () -> animalService.createAnimal(request));
    }

    @Test
    void testUpdateAnimalSuccess() throws ParseException {
        AnimalRequest request = new AnimalRequest();
        request.setName("Tiger");
        request.setTypeName("Wild");
        request.setGender("Female");
        request.setDateOfEntry(parseDate("2024-01-01"));

        Type type = new Type();
        Animal existingAnimal = new Animal();
        existingAnimal.setId(1L);

        when(typeRepository.findByName(anyString())).thenReturn(Optional.of(type));
        when(animalRepository.findById(anyLong())).thenReturn(Optional.of(existingAnimal));
        when(animalRepository.save(any(Animal.class))).thenReturn(existingAnimal);
        animalService.updateAnimal(1L, request);
        verify(animalRepository).save(existingAnimal);
    }

    @Test
    void testUpdateAnimalTypeNotFound() {
        AnimalRequest request = new AnimalRequest();
        request.setTypeName("Unknown");
        when(typeRepository.findByName(anyString())).thenReturn(Optional.empty());
        assertThrows(IllegalArgumentException.class, () -> animalService.updateAnimal(1L, request));
    }

    @Test
    void testUpdateAnimalNotFound() {
        AnimalRequest request = new AnimalRequest();
        request.setTypeName("Wild");
        when(typeRepository.findByName(anyString())).thenReturn(Optional.of(new Type()));
        when(animalRepository.findById(anyLong())).thenReturn(Optional.empty());
        assertThrows(IllegalArgumentException.class, () -> animalService.updateAnimal(1L, request));
    }

    @Test
    void testDeleteAnimalSuccess() {
        when(animalRepository.existsById(anyLong())).thenReturn(true);
        animalService.deleteAnimal(1L);
        verify(animalRepository).deleteById(1L);
    }

    @Test
    void testDeleteAnimalNotFound() {
        when(animalRepository.existsById(anyLong())).thenReturn(false);
        assertThrows(IllegalArgumentException.class, () -> animalService.deleteAnimal(1L));
    }
}