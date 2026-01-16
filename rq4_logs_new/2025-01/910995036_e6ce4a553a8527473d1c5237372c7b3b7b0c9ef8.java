package com.example.lifeonhana.service;

import org.springframework.stereotype.Service;

import com.example.lifeonhana.dto.response.LifeProductResponseDTO;
import com.example.lifeonhana.entity.Product;
import com.example.lifeonhana.global.exception.BadRequestException;
import com.example.lifeonhana.repository.LifeProductRepository;

import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class LifeProductService {
	private final LifeProductRepository lifeProductRepository;

	public LifeProductResponseDTO getLifeProduct(Long productId) {
		Product product = lifeProductRepository.findByProductId(productId).orElseThrow(() -> new BadRequestException(
			"존재하지 않는 id 입니다."));

		if(product.getCategory() != Product.Category.LIFE) {
			throw new BadRequestException("존재하지 않는 id 입니다.");
		}

		return LifeProductResponseDTO.fromEntity(product);
	}
}