package com.jia.study_tracker.service;

import com.jia.study_tracker.domain.Summary;
import com.jia.study_tracker.domain.SummaryType;
import com.jia.study_tracker.domain.User;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.time.LocalDate;
import java.util.Map;

/**
 *  요약 메시지를 Slack으로 전송하며 오류 발생 시 자동 재시도
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class SlackNotificationService {

    private final WebClient slackWebClient;

    /**
     * 정상 요약 메시지를 슬랙으로 전송
     */
    public void sendSummaryToUser(User user, Summary summary) {
        String message = String.format(
                "[%s 요약 📚]\n%s\n\n🌟 피드백:\n%s",
                summary.getType(),
                summary.getSummary(),
                summary.getFeedback() != null ? summary.getFeedback() : "피드백 없음"
        );

        slackWebClient.post()
                .uri("/chat.postMessage")
                .body(BodyInserters.fromValue(
                        Map.of("channel", user.getSlackUserId(), "text", message)
                ))
                .retrieve()
                .bodyToMono(String.class)
                .timeout(Duration.ofSeconds(5))
                .retryWhen(Retry.backoff(3, Duration.ofSeconds(1)))
                .doOnSuccess(resp -> log.debug("✅ Slack 전송 완료: userId={}", user.getSlackUserId()))
                .doOnError(err -> log.warn("❌ Slack 전송 실패: userId={}, reason={}", user.getSlackUserId(), err.getMessage()))
                .subscribe();
    }

    /**
     * 요약 생성 실패 시 사용자에게 관리자 문의 안내 메시지를 전송
     */
    public void sendErrorNotice(User user, LocalDate date, SummaryType type) {
        String errorMessage = String.format(
                "[%s 요약 ⚠️]\n%s님의 %s 요약 생성 중 오류가 발생했습니다.\n" +
                        "반복해서 이 메시지를 받으신다면 관리자에게 문의해주세요.",
                type, user.getSlackUsername(), date
        );

        slackWebClient.post()
                .uri("/chat.postMessage")
                .body(BodyInserters.fromValue(
                        Map.of("channel", user.getSlackUserId(), "text", errorMessage)
                ))
                .retrieve()
                .bodyToMono(String.class)
                .timeout(Duration.ofSeconds(5))
                .retryWhen(Retry.backoff(2, Duration.ofSeconds(2)))
                .doOnSuccess(resp -> log.debug("✅ 관리자 문의 안내 전송 완료: userId={}", user.getSlackUserId()))
                .doOnError(err -> log.warn("❌ 관리자 문의 안내 전송 실패: userId={}, reason={}", user.getSlackUserId(), err.getMessage()))
                .subscribe();
    }
}