package org.spiceboys.Travel.Diary.controller;

import org.spiceboys.Travel.Diary.service.CountryService;
import org.spiceboys.Travel.Diary.model.Country;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;

@RestController
@RequestMapping("/api/countries")
public class CountryController {
    private final CountryService countryService;

    public CountryController(CountryService countryService){
        this.countryService = countryService;
    }

    @PostMapping
    public ResponseEntity<Country> createCountry(@RequestBody Country country){
        Country createdCountry = countryService.createCountry(country);
        return ResponseEntity.status(HttpStatus.CREATED).body(createdCountry);
    }
}