package com.example.finalproject.controller;

import com.example.finalproject.model.Customer;
import com.example.finalproject.model.Employee;
import com.example.finalproject.service.CustomerService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1/customer")
@RequiredArgsConstructor
public class CustomerController {
    private final CustomerService customerService;
    @GetMapping("/getAll")
    public ResponseEntity getAll() {
        return ResponseEntity.status(HttpStatus.OK).body(customerService.getAll());
    }

    @GetMapping("/get/{id}")
    public ResponseEntity getById(@PathVariable Integer id) {
        return ResponseEntity.status(HttpStatus.OK).body(customerService.getById(id));
    }

    @PostMapping("/add")
    public ResponseEntity add(@RequestBody @Valid Customer customer) {
        customerService.add(customer);
        return ResponseEntity.status(HttpStatus.CREATED).body("CREATED");
    }

    @PutMapping("/update/{id}")
    public ResponseEntity update(@RequestBody @Valid Customer customer, @PathVariable Integer id) {
        customerService.update(customer, id);
        return ResponseEntity.status(HttpStatus.CREATED).body("UPDATED");
    }

    @DeleteMapping("/delete/{id}")
    public ResponseEntity delete(@PathVariable Integer id) {
        customerService.delete(id);
        return ResponseEntity.status(HttpStatus.OK).body("DELETED");
    }
}