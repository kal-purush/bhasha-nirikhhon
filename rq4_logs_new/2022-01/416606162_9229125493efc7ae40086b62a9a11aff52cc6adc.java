package com.itrex.java.lab.controller;

import com.itrex.java.lab.service.FillingDatabaseTestService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/add")
@RequiredArgsConstructor
public class TestDatabaseFillingController {

    private final FillingDatabaseTestService service;

    @PostMapping("/users")
    @PreAuthorize("hasAuthority('database:fill')")
    public ResponseEntity addUsers(@RequestParam(name = "role") String role,
                                   @RequestParam(name = "userAmount") Integer userAmount) {

        boolean result = service.fillUserTable(role, userAmount);
        return result ? new ResponseEntity(HttpStatus.CREATED) : new ResponseEntity<>(HttpStatus.NOT_MODIFIED);
    }

    @PostMapping("/contracts")
    @PreAuthorize("hasAuthority('database:fill')")
    public ResponseEntity addContracts(@RequestParam(name = "minContractAmount") Integer minContractAmount,
                                       @RequestParam(name = "maxContractAmount") Integer maxContractAmount) {

        boolean result = service.fillContractTableForEachCustomer(minContractAmount, maxContractAmount);
        return result ? new ResponseEntity(HttpStatus.CREATED) : new ResponseEntity<>(HttpStatus.NOT_MODIFIED);
    }

    @PostMapping("/offers")
    @PreAuthorize("hasAuthority('database:fill')")
    public ResponseEntity addOffers(@RequestParam(name = "minOfferAmount") Integer minOfferAmount,
                                       @RequestParam(name = "maxOfferAmount") Integer maxOfferAmount) {

        boolean result = service.fillOffersForEachContract(minOfferAmount, maxOfferAmount);
        return result ? new ResponseEntity(HttpStatus.CREATED) : new ResponseEntity<>(HttpStatus.NOT_MODIFIED);
    }
}