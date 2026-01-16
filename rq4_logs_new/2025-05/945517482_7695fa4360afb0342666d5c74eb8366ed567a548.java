package com.gitprism.GitPRism.portfolios.controller;

import com.gitprism.GitPRism.portfolios.service.EditHistoryRedisService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/portfolios")
@Tag(name = "포트폴리오 편집 기록 API", description = "Redis에 저장된 실시간 편집 이력을 조회하는 기능을 제공합니다.")
public class PortfolioEditController {

  private final EditHistoryRedisService editHistoryRedisService;

  @Operation(summary = "편집 기록 조회", description = "지정된 포트폴리오 ID의 실시간 편집 기록을 Redis에서 조회합니다.")
  @GetMapping("/{portfolioId}/edit-histories")
  public ResponseEntity<List<Object>> getEditHistories(@PathVariable Long portfolioId) {
    List<Object> history = editHistoryRedisService.getHistory(portfolioId);
    return ResponseEntity.ok(history);
  }
}