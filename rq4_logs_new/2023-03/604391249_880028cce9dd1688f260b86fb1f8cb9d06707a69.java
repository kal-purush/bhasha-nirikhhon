package com.abneco.delivery.user.controller;

import com.abneco.delivery.user.json.buyer.BuyerForm;
import com.abneco.delivery.user.json.buyer.BuyerResponse;
import com.abneco.delivery.user.service.BuyerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/buyer")
public class BuyerController {

    @Autowired
    private BuyerService service;

    @GetMapping("")
    public ResponseEntity<List<BuyerResponse>> getAllBuyers() {
        List<BuyerResponse> response = service.findAllBuyers();
        if (response.isEmpty()) {
            return ResponseEntity.noContent().build();
        }
        return ResponseEntity.ok(response);
    }

    @PostMapping("")
    @ResponseStatus(HttpStatus.CREATED)
    public void registerBuyer(@RequestBody BuyerForm form) {
        service.registerBuyer(form);
    }
}