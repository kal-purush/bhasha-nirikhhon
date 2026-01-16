package com.jia.study_tracker.scheduler;

import com.jia.study_tracker.domain.SummaryType;
import com.jia.study_tracker.service.SummaryGenerationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDate;

/**
 * 매월 1일 밤 8시 스케줄러 작동
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class MonthlySummaryScheduler {

    private final SummaryGenerationService summaryGenerationService;

    @Scheduled(cron = "0 0 20 1 * *")
    public void generateMonthlySummaries() {
        LocalDate today = LocalDate.now();
        summaryGenerationService.generateSummaries(today, SummaryType.MONTHLY);
    }
}
