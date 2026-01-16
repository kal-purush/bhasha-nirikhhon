package com.example.lifeonhana.controller;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.example.lifeonhana.ApiResult;
import com.example.lifeonhana.dto.request.LumpSumRequestDTO;
import com.example.lifeonhana.dto.response.LumpSumResponseDTO;
import com.example.lifeonhana.service.LumpSumService;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.AllArgsConstructor;

@RestController
@AllArgsConstructor
@RequestMapping("/api/lumpsum")
@Tag(name = "Lump Sum API", description = "목돈 신청 API")
public class LumpSumController {
	private final LumpSumService lumpSumService;

	@PostMapping("")
	@Operation(summary = "목돈 신청", description = "목돈 신청을 처리합니다.")
	@ApiResponses(value = {
		@ApiResponse(responseCode = "200", description = "목돈 인출 신청 성공"),
		@ApiResponse(responseCode = "400", description = "잘못된 요청입니다.")
	})
	@SecurityRequirement(name = "bearerAuth")
	public ResponseEntity<ApiResult> createLumpSum(
		@AuthenticationPrincipal String authId,
		@RequestBody LumpSumRequestDTO requestDTO) {
		LumpSumResponseDTO responseDTO = lumpSumService.createLumpSum(authId, requestDTO);
		return ResponseEntity.ok(
			new ApiResult(200, HttpStatus.OK, "목돈 인출 신청 성공", responseDTO)
		);
	}
}