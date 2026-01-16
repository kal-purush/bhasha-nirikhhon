package com.jia.study_tracker.service;

import com.jia.study_tracker.domain.Summary;
import com.jia.study_tracker.domain.SummaryType;
import com.jia.study_tracker.domain.User;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.web.reactive.function.client.WebClient;

import java.io.IOException;
import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.*;

/**
 * SlackNotificationService 테스트
 *
 * 목표:
 * - SlackNotificationService가 Slack API에 적절한 메시지를 전송하는지 검증한다.
 * - 실제 Slack 호출 없이 MockWebServer를 통해 요청 내용을 검증한다.
 *
 * 테스트 시나리오:
 * 1. sendSummary 메서드가 요약 및 피드백을 포함한 Slack 메시지를 정상 전송하는지 확인한다.
 * 2. sendErrorNotice 메서드가 요약 실패 알림 메시지를 사용자에게 전송하는지 확인한다.
 */
class SlackNotificationServiceTest {

    private MockWebServer mockWebServer;
    private SlackNotificationService slackNotificationService;

    @BeforeEach
    void setUp() throws IOException {
        mockWebServer = new MockWebServer();
        mockWebServer.start();

        WebClient webClient = WebClient.builder()
                .baseUrl(mockWebServer.url("/").toString())
                .build();

        slackNotificationService = new SlackNotificationService(webClient);
    }

    @AfterEach
    void tearDown() throws IOException {
        mockWebServer.shutdown();
    }

    @Test
    @DisplayName("정상적인 요약 메시지를 슬랙으로 전송한다")
    void sendSummaryToUser_shouldSendFormattedSlackMessage() throws InterruptedException {
        // given
        User user = new User("U123", "testuser");
        Summary summary = new Summary(LocalDate.now(), "오늘 한 공부", "잘했어요!", true, null, user, SummaryType.DAILY);
        mockWebServer.enqueue(new MockResponse().setBody("{\"ok\":true}").setResponseCode(200));

        // when
        slackNotificationService.sendSummaryToUser(user, summary);

        // then
        var recordedRequest = mockWebServer.takeRequest();
        assertEquals("/chat.postMessage", recordedRequest.getPath());
        String body = recordedRequest.getBody().readUtf8();
        assertTrue(body.contains("오늘 한 공부"));
        assertTrue(body.contains("잘했어요!"));
    }

    @Test
    @DisplayName("요약 생성 실패 메시지를 슬랙으로 전송한다")
    void sendErrorNotice_shouldSendErrorSlackMessage() throws InterruptedException {
        // given
        User user = new User("U123", "testuser");
        mockWebServer.enqueue(new MockResponse().setBody("{\"ok\":true}").setResponseCode(200));

        // when
        slackNotificationService.sendErrorNotice(user, LocalDate.of(2025, 5, 10), SummaryType.WEEKLY);

        // then
        var recordedRequest = mockWebServer.takeRequest();
        assertEquals("/chat.postMessage", recordedRequest.getPath());
        String body = recordedRequest.getBody().readUtf8();
        assertTrue(body.contains("요약 생성 중 오류"));
        assertTrue(body.contains("testuser"));
    }

}