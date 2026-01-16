package com.jia.study_tracker.scheduler;

import com.jia.study_tracker.domain.DailySummary;
import com.jia.study_tracker.domain.StudyLog;
import com.jia.study_tracker.domain.User;
import com.jia.study_tracker.repository.DailySummaryRepository;
import com.jia.study_tracker.repository.UserRepository;
import com.jia.study_tracker.service.OpenAIClient;
import com.jia.study_tracker.service.StudyLogQueryService;
import com.jia.study_tracker.service.dto.SummaryResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.util.List;

/**
 * 매일 정해진 시간(밤 10시)에 모든 사용자들의 학습 로그를 조회하고,
 * OpenAI를 통해 학습 요약 및 동기부여 피드백을 생성하여 저장하는 스케줄러 컴포넌트
 *
 * 주요 책임:
 * - 전체 사용자 조회
 * - 각 사용자의 당일 학습 로그 가져오기
 * - OpenAI API 호출을 통해 요약 및 피드백 생성
 * - 결과를 DailySummary 엔티티로 저장
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class DailySummaryScheduler {

    private final UserRepository userRepository; // 전체 사용자 조회 목적
    private final StudyLogQueryService studyLogQueryService; // 날짜별 학습 로그 조회 목적
    private final OpenAIClient openAIClient; // OpenAI API를 호출하여 요약 및 피드백을 생성 목적
    private final DailySummaryRepository dailySummaryRepository; // // 생성된 요약 및 피드백을 저장하기 위한 목적

    @Scheduled(cron = "0 0 22 * * *") // 매일 밤 10시 실행
    public void generateDailySummaries() {
        LocalDate today = LocalDate.now();
        List<User> users = userRepository.findAll();

        for (User user : users) {
            // 사용자의 오늘 학습 로그 조회
            List<StudyLog> logs = studyLogQueryService.getDailyLogs(user.getSlackUserId(), today);

            // 로그가 없는 경우 스킵
            if (logs.isEmpty()) {
                log.info("❌ [{}] 오늘의 로그 없음 - 요약 생략", user.getSlackUsername());
                continue;
            }

            // GPT를 이용한 요약 및 피드백 생성
            SummaryResult result = openAIClient.generateSummaryAndFeedback(logs);

            // 결과를 엔티티로 변환하여 저장
            DailySummary summary = new DailySummary(
                    today,
                    result.getSummary(),
                    result.getFeedback(),
                    true,
                    null,
                    user
            );
            dailySummaryRepository.save(summary);

            log.info("✅ [{}] 요약/피드백 저장 완료", user.getSlackUsername());
        }
    }
}