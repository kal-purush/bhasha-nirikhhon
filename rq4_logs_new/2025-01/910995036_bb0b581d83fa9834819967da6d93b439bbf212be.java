package com.example.lifeonhana.service;

import org.springframework.stereotype.Service;

import com.example.lifeonhana.dto.response.SavingsProductResponseDTO;
import com.example.lifeonhana.entity.Product;
import com.example.lifeonhana.global.exception.BadRequestException;
import com.example.lifeonhana.repository.SavingsProductRepository;

import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class SavingsProductService {
	private final SavingsProductRepository savingsProductRepository;

	public SavingsProductResponseDTO getSavingsProduct(Long productId) {
		Product product =
			savingsProductRepository.findProductByProductId(productId)
				.orElseThrow(() -> new BadRequestException("존재하지 않는 id 입니다."));

		if (product.getCategory() != Product.Category.SAVINGS) {
			throw new BadRequestException("존재하지 않는 id 입니다.");
		}

		return new SavingsProductResponseDTO(
			product.getProductId(),
			product.getName(),
			product.getDescription(),
			"https://example.com/product/" + product.getProductId(),
			new SavingsProductResponseDTO.SavingsInfo(
				product.getBasicInterestRate(),
				product.getMaxInterestRate()
			)
		);
	}
}