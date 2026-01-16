package dev.forkingaround.zootopia.controllers;

import dev.forkingaround.zootopia.models.Type;
import dev.forkingaround.zootopia.repositories.TypeRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Optional;

@RestController
@RequestMapping("/api/types")
public class TypeController {

    @Autowired
    private TypeRepository typeRepository;

    @GetMapping
    public ResponseEntity<List<Type>> getAllTypes() {
        List<Type> types = typeRepository.findAll();
        return ResponseEntity.ok(types);
    }

    @GetMapping("/{id}")
    public ResponseEntity<Type> getTypeById(@PathVariable Long id) throws Exception {
        Optional<Type> type = typeRepository.findById(id);
        if (type.isPresent()) {
            return ResponseEntity.ok(type.get());
        } else {
            throw new Exception("Type not found with ID " + id);
        }
    }

    @PostMapping("/add")
    public ResponseEntity<Type> createType(@RequestBody Type newType) {
        Type createdType = typeRepository.save(newType);
        return ResponseEntity.status(HttpStatus.CREATED).body(createdType);
    }

    @PutMapping("update/{id}")
    public ResponseEntity<Type> updateType(@PathVariable Long id, @RequestBody Type updatedType) throws Exception {
        if (!typeRepository.existsById(id)) {
            throw new Exception("Type not found with ID " + id);
        }
        updatedType.setId(id);
        Type type = typeRepository.save(updatedType);
        return ResponseEntity.ok(type);
    }

    @DeleteMapping("delete/{id}")
    public ResponseEntity<Void> deleteType(@PathVariable Long id) throws Exception {
        if (!typeRepository.existsById(id)) {
            throw new Exception("Type not found with ID " + id);
        }
        typeRepository.deleteById(id);
        return ResponseEntity.noContent().build();
    }
}