package com.example.lifeonhana.controller;

import com.example.lifeonhana.ApiResult;
import com.example.lifeonhana.dto.HistoryDTO.HistoryResponseDTO;
import com.example.lifeonhana.dto.HistoryDTO.MonthlyExpenseResponseDTO;
import com.example.lifeonhana.dto.HistoryDTO.StatisticsResponseDTO;
import com.example.lifeonhana.service.HistoryService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/history")
@RequiredArgsConstructor
@Tag(name = "History", description = "거래 내역 API")
public class HistoryController {
	private final HistoryService historyService;

	@Operation(summary = "입출금 내역 조회", description = "특정 년월의 입출금 내역을 조회합니다.")
	@ApiResponses({
		@ApiResponse(responseCode = "200", description = "거래 내역 조회 성공"),
		@ApiResponse(responseCode = "400", description = "올바른 년월 형식이 아닙니다. (YYYYMM)"),
		@ApiResponse(responseCode = "401", description = "인증이 필요합니다.")
	})
	@GetMapping
	public ResponseEntity<ApiResult> getHistories(
		@Parameter(description = "조회할 년월 (YYYYMM 형식)", required = true)
		@RequestParam String yearMonth,

		@Parameter(description = "페이지 번호 (1부터 시작)", example = "1")
		@RequestParam(defaultValue = "1") int page,

		@Parameter(description = "한 페이지당 항목 수", example = "20")
		@RequestParam(defaultValue = "20") int size,

		@Parameter(hidden = true)
		@AuthenticationPrincipal String authId
	) {
		HistoryResponseDTO response = historyService.getHistories(yearMonth, authId, page, size);
		return ResponseEntity.ok(ApiResult.builder()
			.code(HttpStatus.OK.value())
			.status(HttpStatus.OK)
			.message("거래 내역 조회 성공")
			.data(response)
			.build());
	}

	@Operation(summary = "월별 지출 내역 조회", description = "최근 5개월의 월별 지출 내역을 조회합니다.")
	@ApiResponses({
		@ApiResponse(responseCode = "200", description = "월별 지출 내역 조회 성공"),
		@ApiResponse(responseCode = "401", description = "인증이 필요합니다.")
	})
	@GetMapping("/monthly")
	public ResponseEntity<ApiResult> getMonthlyExpenses(
		@Parameter(hidden = true)
		@AuthenticationPrincipal String authId
	) {
		MonthlyExpenseResponseDTO response = historyService.getMonthlyExpenses(authId);
		return ResponseEntity.ok(ApiResult.builder()
			.code(HttpStatus.OK.value())
			.status(HttpStatus.OK)
			.message("월별 지출 내역 조회 성공")
			.data(response)
			.build());
	}

	@Operation(summary = "거래 통계 조회", description = "특정 년월의 거래 통계를 조회합니다.")
	@ApiResponses({
		@ApiResponse(responseCode = "200", description = "거래 통계 조회 성공"),
		@ApiResponse(responseCode = "400", description = "올바른 년월 형식이 아닙니다. (YYYYMM)"),
		@ApiResponse(responseCode = "401", description = "인증이 필요합니다.")
	})
	@GetMapping("/statistics")
	public ResponseEntity<ApiResult> getStatistics(
		@Parameter(description = "조회할 년월 (YYYYMM 형식)", example = "202401", required = true)
		@RequestParam String yearMonth,

		@Parameter(hidden = true)
		@AuthenticationPrincipal String authId
	) {
		StatisticsResponseDTO response = historyService.getStatistics(yearMonth, authId);
		return ResponseEntity.ok(ApiResult.builder()
			.code(HttpStatus.OK.value())
			.status(HttpStatus.OK)
			.message("거래 통계 조회 성공")
			.data(response)
			.build());
	}
}