package com.example.lifeonhana.controller;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.example.lifeonhana.ApiResult;
import com.example.lifeonhana.dto.response.ProductListResponseDTO;
import com.example.lifeonhana.dto.response.SimpleProductResponseDTO;
import com.example.lifeonhana.service.ProductService;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;

@RestController
@RequestMapping("/api/products")
@Tag(name = "Products List API", description = "상품 목록 api")
public class ProductController {

	private final ProductService productService;

	public ProductController(ProductService productService) {
		this.productService = productService;
	}

	@GetMapping("")
	@Operation(summary = "상품 목록 조회", description = "상품 목록을 조회합니다.")
	@ApiResponses(value = {
		@ApiResponse(responseCode = "200", description = "상품 목록 조회 성공"),
		@ApiResponse(responseCode = "400", description = "잘못된 요청입니다.")
	})
	@SecurityRequirement(name = "bearerAuth")
	public ResponseEntity<ApiResult> getProducts(
		@RequestParam(value = "category", required = false) String category,
		@RequestParam(defaultValue = "1") int offset,
		@RequestParam(defaultValue = "20") int limit) {
		ProductListResponseDTO<SimpleProductResponseDTO> productResponse = productService.getProducts(category, offset,
			limit);
		return ResponseEntity.ok(new ApiResult(200, HttpStatus.OK, "상품 목록 조회 성공", productResponse));
	}

	// @GetMapping("/savings/{productId}")

	// @GetMapping("/loans/{productId}")

	// @GetMapping("/life/{productId}")

}