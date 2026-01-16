package dev.forkingaround.zootopia.services;

import java.util.List;
import java.util.Optional;

import dev.forkingaround.zootopia.dtos.AnimalRequest;
import dev.forkingaround.zootopia.models.Animal;
import dev.forkingaround.zootopia.models.Type;
import dev.forkingaround.zootopia.repositories.AnimalRepository;
import dev.forkingaround.zootopia.repositories.TypeRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import jakarta.transaction.Transactional;


@Service
public class AnimalService {

    @Autowired
    private AnimalRepository animalRepository;

    @Autowired
    private TypeRepository typeRepository;

    @Transactional
    public List<Animal> getAllAnimals() {
        return animalRepository.findAll();
    }

    @Transactional
    public Optional<Animal> getAnimalById(Long id) {
        return animalRepository.findById(id);
    }

    @Transactional
public Animal createAnimal(AnimalRequest request) {
    Type type = typeRepository.findByName(request.getTypeName());
    if (type == null) {
        throw new IllegalArgumentException("Type not found");
    }

    Animal animal = Animal.builder()
        .name(request.getName())
        .type(type)
        .gender(request.getGender())
        .dateOfEntry(request.getDateOfEntry())
        .build();

    return animalRepository.save(animal);
}
    @Transactional
    public void updateAnimal(Long id, AnimalRequest request) {
        Type type = typeRepository.findByName(request.getTypeName());
        if (type == null) {
            throw new IllegalArgumentException("Type not found");
        }

        Animal animal = animalRepository.findById(id)
            .orElseThrow(() -> new IllegalArgumentException("Animal not found"));

        animal.setName(request.getName());
        animal.setType(type);
        animal.setGender(request.getGender());
        animal.setDateOfEntry(request.getDateOfEntry());

        animalRepository.save(animal);
    }

    @Transactional
    public void deleteAnimal(Long id) {
        if (!animalRepository.existsById(id)) {
            throw new IllegalArgumentException("Animal not found");
        }
        animalRepository.deleteById(id);
    }
}