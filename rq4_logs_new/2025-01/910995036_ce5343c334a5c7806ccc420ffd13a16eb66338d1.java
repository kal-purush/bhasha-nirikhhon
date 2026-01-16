package com.example.lifeonhana.controller;
import lombok.RequiredArgsConstructor;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.*;

import com.example.lifeonhana.ApiResult;
import com.example.lifeonhana.dto.request.ProductInsightRequest;
import com.example.lifeonhana.dto.response.ProductInsightResponse;
import com.example.lifeonhana.service.ProductInsightService;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/anthropic")
public class ProductInsightController {

	private final ProductInsightService productInsightService;

	@PostMapping("/effect")
	public ResponseEntity<ApiResult> getProductInsight(
		@RequestBody ProductInsightRequest request,
		@AuthenticationPrincipal String authId
	) {
		try {
			ProductInsightResponse insightResponse = productInsightService.getProductInsight(request, authId);

			return ResponseEntity.ok(new ApiResult(
				200,
				HttpStatus.OK,
				"상품 분석 성공",
				insightResponse
			));
		} catch (IllegalArgumentException e) {
			return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(new ApiResult(
				400,
				HttpStatus.BAD_REQUEST,
				e.getMessage(),
				null
			));
		} catch (Exception e) {
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(new ApiResult(
				500,
				HttpStatus.INTERNAL_SERVER_ERROR,
				"내부 서버 오류 발생: " + e.getMessage(),
				null
			));
		}
	}
}