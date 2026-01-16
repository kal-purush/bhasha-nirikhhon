package com.example.finport.controller;

import com.example.finport.entity.Stock;
import com.example.finport.repository.StockRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/stock")
@RequiredArgsConstructor
public class StockApiController {
    private final StockRepository stockRepository;

    @GetMapping("/{stockId}/price-info")
    public ResponseEntity<Map<String, Object>> getStockInfo(@PathVariable Long stockId) {
        Stock stock = stockRepository.findById(stockId).orElse(null);
        if (stock == null) return ResponseEntity.notFound().build();

        Map<String, Object> data = new HashMap<>();
        data.put("currentPrice", stock.getCurrentPrice());
        data.put("upperLimitPrice", stock.getUpperLimitPrice());
        data.put("lowerLimitPrice", stock.getLowerLimitPrice());

        return ResponseEntity.ok(data);
    }
}