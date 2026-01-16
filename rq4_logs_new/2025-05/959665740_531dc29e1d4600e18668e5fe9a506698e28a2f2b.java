package com.example.backend.controllers;


import com.example.backend.dtos.ApartmentDTO;
import com.example.backend.dtos.ApartmentDTO;
import com.example.backend.services.ApartmentService;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/apartments")
@RequiredArgsConstructor
public class ApartmentController {

    private final ApartmentService apartmentService;


    @GetMapping
    public ResponseEntity<Page<ApartmentDTO>> findAll(@RequestParam(value = "page", defaultValue = "1") int page,
                                                      @RequestParam(value = "limit", defaultValue = "10") int limit) {
        Pageable pageable = PageRequest.of(page - 1, limit);
        return ResponseEntity.ok(apartmentService.getAllApartments(pageable));
    }

    @PostMapping
    public ResponseEntity<ApartmentDTO> saveApartment(@RequestBody JsonNode data) {
        ApartmentDTO savedApartment = apartmentService.saveApartment(data);
        return new ResponseEntity<>(savedApartment, HttpStatus.CREATED);
    }
}