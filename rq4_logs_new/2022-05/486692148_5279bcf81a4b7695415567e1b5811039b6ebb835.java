package com.example.storeeverything.Information;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.NoSuchElementException;

@CrossOrigin
@RestController
@RequestMapping("/informations")
public class InformationController {

    private final InformationRepository informationRepository;

    @Autowired
    public InformationController(InformationRepository repository) {
        this.informationRepository = repository;
    }

    @GetMapping("")
    public ResponseEntity<List<Information>> getAll() {
        return new ResponseEntity<>(informationRepository.findAll(), HttpStatus.OK);
    }

    @GetMapping("/{id}")
    public ResponseEntity<Information> getById(@PathVariable int id) {
        try {
            Information information = informationRepository.findById(id).orElseThrow();
            return new ResponseEntity<>(information, HttpStatus.OK);
        } catch (NoSuchElementException e) {
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }
    }

    @PostMapping("")
    public ResponseEntity<Information> add(@RequestBody Information data) {
        data.setId(0);
        if (! informationRepository.findAll().contains(data)) {
            informationRepository.save(data);
            return new ResponseEntity<>(data, HttpStatus.CREATED);
        }
        return new ResponseEntity<>(HttpStatus.CONFLICT);
    }

    @DeleteMapping("/{id}")
    ResponseEntity<Information> delete(@PathVariable int id) {
        try {
            Information information = informationRepository.findById(id).orElseThrow();
            informationRepository.deleteById(id);
            return new ResponseEntity<>(information, HttpStatus.OK);
        } catch (NoSuchElementException e) {
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }
    }

    @PutMapping("/{id}")
    ResponseEntity<Information> put(@RequestBody Information data, @PathVariable int id) {
        data.setId(id);
        informationRepository.save(data);
        return new ResponseEntity<>(data, HttpStatus.OK);
    }
}