package pl.maks.carrental.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import pl.maks.carrental.service.CarRentalService;

import java.math.BigDecimal;

@RestController
public class CarRentalController {

    private final CarRentalService carRentalService;

    @Autowired
    public CarRentalController(CarRentalService carRentalService) {
        this.carRentalService = carRentalService;
    }

    @GetMapping("/calculateRentalPrice")
    public ResponseEntity<String> calculateRentalPrice(
            @RequestParam Integer client,
            @RequestParam Integer car,
            @RequestParam Integer days) {

        try {
            BigDecimal rentalPrice = carRentalService.calculateRentalPrice(client, car, days);
            return ResponseEntity.ok("Rental price: " + rentalPrice.toString());
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(e.getMessage());
        }
    }
}