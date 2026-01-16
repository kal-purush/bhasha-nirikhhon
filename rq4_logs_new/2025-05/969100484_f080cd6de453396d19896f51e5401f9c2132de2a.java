package com.jia.study_tracker;

import com.jia.study_tracker.domain.StudyLog;
import com.jia.study_tracker.service.StudyLogQueryService;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.util.List;

@Component
@RequiredArgsConstructor
public class LogTestRunner implements CommandLineRunner {

    private final StudyLogQueryService studyLogQueryService;

    @Override
    public void run(String... args) throws Exception {
        List<StudyLog> dailyLogs = studyLogQueryService.getDailyLogs("U123456", LocalDate.of(2025, 5, 2));
        dailyLogs.forEach(log -> System.out.println("✅ 로그: " + log.getContent()));

        List<StudyLog> weeklyLogs = studyLogQueryService.getWeeklyLogs("U123456", LocalDate.of(2025, 4, 29)); // 주 시작일
        System.out.println("📅 주간 로그 개수: " + weeklyLogs.size());
    }
}