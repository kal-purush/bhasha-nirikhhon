package com.example.lifeonhana.controller;

import java.math.BigDecimal;
import java.util.List;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.example.lifeonhana.ApiResult;
import com.example.lifeonhana.dto.request.LoanRecommendationRequest;
import com.example.lifeonhana.dto.response.LoanProductResponse;
import com.example.lifeonhana.service.LoanRecommendationService;

import lombok.RequiredArgsConstructor;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/anthropic/loans")
public class LoanRecommendationController {

	private final LoanRecommendationService loanRecommendationService;

	@PostMapping
	public ResponseEntity<ApiResult> recommendLoanProducts(
		@RequestBody LoanRecommendationRequest request,
		@AuthenticationPrincipal String authId
	) {
		try {
			String reason = request.reason();
			BigDecimal amount = request.amount();

			List<LoanProductResponse> recommendedProducts = loanRecommendationService.recommendLoanProducts(reason, amount, authId);

			return ResponseEntity.ok(new ApiResult(
				200,
				HttpStatus.OK,
				"대출 상품 추천 성공",
				recommendedProducts
			));
		} catch (Exception e) {
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(new ApiResult(
				500,
				HttpStatus.INTERNAL_SERVER_ERROR,
				e.getMessage(),
				null
			));
		}
	}
}