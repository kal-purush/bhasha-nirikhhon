package com.jia.study_tracker.controller;

import com.jia.study_tracker.domain.SummaryType;
import com.jia.study_tracker.service.SummaryGenerationService;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Profile;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;

/**
 * JMeter로 스케줄러를 트리거할 수 없기 때문에 엔드포인트를 노출시켜 수동 호출하는 컨트롤러
 */
@Profile("dev")
@RestController
@RequiredArgsConstructor
public class TestSchedulerController {

    private final SummaryGenerationService summaryGenerationService;

    @PostMapping("/test/run-daily-scheduler")
    public ResponseEntity<String> runScheduler() {
        summaryGenerationService.generateSummaries(LocalDate.now(), SummaryType.DAILY);
        return ResponseEntity.ok("스케줄러 실행 완료");
    }
}