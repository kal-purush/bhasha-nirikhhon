package com.example.lifeonhana.dto.response;

import java.math.BigDecimal;

import com.example.lifeonhana.entity.Product;

public record SavingProductResponseDTO(
	Long productId,
	String name,
	String description,
	String link,
	SavingsInfo savingsInfo
) {
	public record SavingsInfo(
		BigDecimal basicInterestRate,
		BigDecimal maxInterestRate) {
	}

	public static SavingProductResponseDTO fromEntity(Product product) {
		return new SavingProductResponseDTO(
			product.getProductId(),
			product.getName(),
			product.getDescription(),
			"https://example.com/products/savings/" + product.getLink(),
			new SavingProductResponseDTO.SavingsInfo(
				product.getBasicInterestRate(),
				product.getMaxInterestRate()
			)
		);
	}
}