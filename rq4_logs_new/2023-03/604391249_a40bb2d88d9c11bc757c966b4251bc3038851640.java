package com.abneco.delivery.address.controller;

import com.abneco.delivery.address.dto.AddressForm;
import com.abneco.delivery.address.dto.AddressResponse;
import com.abneco.delivery.address.service.AddressService;
import lombok.AllArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/address")
@AllArgsConstructor
public class AddressController {

    @Autowired
    private AddressService service;

    @PostMapping("")
    @ResponseStatus(HttpStatus.CREATED)
    public void registerAddress(@RequestBody AddressForm form) {
        service.registerAddressByCep(form);
    }

    @GetMapping("")
    @ResponseStatus(HttpStatus.OK)
    public ResponseEntity<List<AddressResponse>> getAllAddresses() {
        List<AddressResponse> response = service.getAllAddresses();
        if (response.isEmpty()) {
            return ResponseEntity.noContent().build();
        }
        return ResponseEntity.ok(response);
    }
}