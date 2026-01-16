package com.example.backend.controllers;

import com.example.backend.dtos.FeeDTO;
import com.example.backend.services.FeeService;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/fees")
@RequiredArgsConstructor
public class FeeController {
    private final FeeService feeService;

    @GetMapping
    public ResponseEntity<Page<FeeDTO>> findAll(@RequestParam(value = "page", defaultValue = "1") int page,
                                         @RequestParam(value = "limit", defaultValue = "10") int limit) {
        Pageable pageable = PageRequest.of(page - 1, limit);
        return ResponseEntity.ok(feeService.getAllFees(pageable));
    }

    @GetMapping("/{year}-{month}")
    public ResponseEntity<List<FeeDTO>> findAllByMonth(@PathVariable Integer year, @PathVariable Integer month) {
        return ResponseEntity.ok(feeService.getFeesByMonth(year, month));
    }

    @PostMapping
    public ResponseEntity<Void> save(@RequestBody FeeDTO feeDTO) {
        feeService.addFee(feeDTO);
        return ResponseEntity.ok().build();
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> deleteResident(@PathVariable Long id) {
        feeService.deleteFee(id);
        return ResponseEntity.noContent().build();
    }
}
