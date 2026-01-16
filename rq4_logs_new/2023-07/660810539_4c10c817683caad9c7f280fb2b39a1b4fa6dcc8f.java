package com.practice.studentenrollment.citydistrictapi.controller;
import com.practice.studentenrollment.citydistrictapi.service.CityService;
import com.practice.studentenrollment.citydistrictapi.model.City;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/v1/cities")
public class CityController {
    private final CityService cityService;

    @Autowired
    public CityController(CityService cityService) {
        this.cityService = cityService;
    }

    @GetMapping("/all")
    public ResponseEntity<List<City>> getAllCities() {
        List<City> cities = cityService.getAllCities();
        return ResponseEntity.ok(cities);
    }

    /* @GetMapping("/{id}")
     public ResponseEntity<City> getCityById(@PathVariable int id) {
         Optional<City> optionalCity = cityService.getCityById(id);
         if (optionalCity.isPresent()) {
             City city = optionalCity.get();
             return ResponseEntity.ok(city);
         } else {
             return ResponseEntity.notFound().build();
         }
     }*/
    @GetMapping("/cities/{id}")
    public ResponseEntity<?> getCityById(@PathVariable int id) {
        City city=cityService.getCityById(id);
        if (city == null) {
            return ResponseEntity.status(HttpStatus.OK).body("{\"message\":\"City not found\"}");
        } else {
            return ResponseEntity.ok(city);
        }
    }
}
/* @GetMapping("/{id}")
    public ResponseEntity<?> getDistrictById(@PathVariable int id) {
        District district = districtService.getDistrictById(id);

        if (district == null) {
            return ResponseEntity.status(HttpStatus.OK).body("District not found");
        } else {
            return ResponseEntity.ok(district);
        }
    }*/