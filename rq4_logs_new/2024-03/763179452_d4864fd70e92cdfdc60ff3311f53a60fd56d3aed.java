package com.BackendChallenge.TechTrendEmporium.controller;

import com.BackendChallenge.TechTrendEmporium.controller.Requests.PurchaseRequest;
import com.BackendChallenge.TechTrendEmporium.service.PurchasesService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/employee/purchases")
public class PurchaseController {

    @Autowired
    private PurchasesService purchasesService;

    @GetMapping(value = "all")
    public ResponseEntity<?> getAllPurchases() {
        return ResponseEntity.ok(purchasesService.getAllPurchases());
    }

    @GetMapping()
    public ResponseEntity<?> getPurchasesByUserId(@RequestBody PurchaseRequest request) {
        return ResponseEntity.ok(purchasesService.getPurchasesByUserId(request.getUser_id()));
    }

    @PostMapping
    public ResponseEntity<?> updatePurchaseStatus(@RequestBody PurchaseRequest request) {
        Boolean updated = purchasesService.updatePurchaseStatus(request.getSale_id(), request.getStatus());
        if (updated) {
            return ResponseEntity.ok("Purchase status updated successfully");
        } else {
            return ResponseEntity.badRequest().body("ERROR : Purchase status not updated");
        }
    }
}