package com.example.lifeonhana.dto.response;

import java.math.BigDecimal;

import com.example.lifeonhana.entity.Product;

public record LoanProductDetailResponseDTO(
	Long productId,
	String name,
	String description,
	String feature,
	String target,
	String link,
	LoanInfo loanInfo
) {
	public record LoanInfo(
		BigDecimal minAmount,
		BigDecimal maxAmount,
		BigDecimal basicInterestRate,
		BigDecimal maxInterestRate,
		Integer minPeriod,
		Integer maxPeriod,
		Long minCreditScore
	) {
	}

	public static LoanProductDetailResponseDTO fromEntity(Product product) {
		return new LoanProductDetailResponseDTO(
			product.getProductId(),
			product.getName(),
			product.getDescription(),
			product.getFeature(),
			product.getTarget(),
			"https://example.com/products/loan" + product.getLink(),
			new LoanProductDetailResponseDTO.LoanInfo(
				product.getMinAmount(),
				product.getMaxAmount(),
				product.getBasicInterestRate(),
				product.getMaxInterestRate(),
				product.getMinPeriod(),
				product.getMaxPeriod(),
				product.getMinCreditScore()
			)
		);
	}
}