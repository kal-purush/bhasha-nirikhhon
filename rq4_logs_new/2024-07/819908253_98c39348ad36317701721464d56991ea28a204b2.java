package com.wojciechbarwinski.demo.legendary_warehouse.dtos;

public record InsufficientStockDTO(String productId, int expectedQuantity, int stockQuantity) {
}