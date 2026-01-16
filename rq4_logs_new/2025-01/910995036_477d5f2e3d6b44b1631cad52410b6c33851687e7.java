package com.example.lifeonhana.dto.response;

import java.math.BigDecimal;

import com.example.lifeonhana.entity.Product;

public record ProductResponseDTO (
	Long productId,
	String name,
	String description,
	String category,
	BigDecimal minAmount,
	BigDecimal maxAmount,
	BigDecimal basicInterestRate,
	BigDecimal maxInterestRate,
	Integer maxPeriod,
	Long minCreditScore) {

	public static ProductResponseDTO fromEntity(Product product) {
		return new ProductResponseDTO(
			product.getProductId(),
			product.getName(),
			product.getDescription(),
			product.getCategory().name(),
			product.getMinAmount(),
			product.getMaxAmount(),
			product.getBasicInterestRate(),
			product.getMaxInterestRate(),
			product.getMaxPeriod(),
			product.getMinCreditScore()
		);
	}
}