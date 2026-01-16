package com.polzzak.domain.ranking.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.polzzak.domain.ranking.dto.RakingSummaryListResponse;
import com.polzzak.domain.ranking.service.RankingService;
import com.polzzak.global.common.ApiResponse;

@RestController
@RequestMapping("/api/v1/rankings")
public class RankingController {
	private final RankingService rankingService;

	public RankingController(final RankingService rankingService) {
		this.rankingService = rankingService;
	}

	@GetMapping("/guardians")
	public ResponseEntity<ApiResponse<RakingSummaryListResponse>> getGuardianRankingSummaries() {
		return ResponseEntity.ok(ApiResponse.ok(rankingService.getGuardianRankingSummaries()));
	}

	@GetMapping("/kids")
	public ResponseEntity<ApiResponse<RakingSummaryListResponse>> getKidRankingSummaries() {
		return ResponseEntity.ok(ApiResponse.ok(rankingService.getKidRankingSummaries()));
	}
}