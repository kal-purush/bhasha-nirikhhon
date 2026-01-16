package com.jia.study_tracker.service;

import com.jia.study_tracker.domain.StudyLog;
import com.jia.study_tracker.domain.User;
import com.jia.study_tracker.exception.InvalidOpenAIResponseException;
import com.jia.study_tracker.service.dto.SummaryResult;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.reactive.function.client.WebClient;

import java.time.LocalDateTime;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * OpenAIClient 테스트
 *
 * 목표:
 * - OpenAIClient의 generateSummaryAndFeedback 메서드가
 *   정상 응답을 잘 파싱하는지,
 *   잘못된 응답은 예외로 처리하는지 검증한다.
 *
 * 테스트 시나리오:
 * 1. 정상적인 OpenAI 응답을 받으면 summary와 feedback을 올바르게 파싱한다.
 * 2. 응답에 '피드백:'이 빠져 있으면 InvalidOpenAIResponseException을 던진다.
 */
class OpenAIClientTest {

    private MockWebServer mockWebServer;
    private OpenAIClient openAIClient;

    @BeforeEach
    void setUp() throws Exception {
        mockWebServer = new MockWebServer();
        mockWebServer.start();

        WebClient webClient = WebClient.builder()
                .baseUrl(mockWebServer.url("/").toString())
                .build();

        openAIClient = new OpenAIClient(webClient);

        ReflectionTestUtils.setField(openAIClient, "model", "gpt-4o-mini");
    }

    @AfterEach
    void tearDown() throws Exception {
        mockWebServer.shutdown();
    }

    @DisplayName("정상 응답이면 요약과 피드백을 잘 파싱한다")
    @Test
    void generateSummaryAndFeedback_success() {
        // given
        String fakeJsonResponse = """
            {
              "choices": [
                {
                  "message": {
                    "content": "요약: 자바 테스트 코드를 학습했습니다.\\n피드백: 꾸준히 연습하고 있어 정말 멋져요!"
                  }
                }
              ]
            }
            """;

        mockWebServer.enqueue(new MockResponse()
                .setResponseCode(200)
                .setBody(fakeJsonResponse)
                .addHeader("Content-Type", "application/json"));

        StudyLog log = new StudyLog("자바 테스트 코드 공부", LocalDateTime.now(), new User("U123", "Jia"));

        // when
        SummaryResult result = openAIClient.generateSummaryAndFeedback(List.of(log));

        // then
        assertThat(result.getSummary()).isEqualTo("자바 테스트 코드를 학습했습니다.");
        assertThat(result.getFeedback()).isEqualTo("꾸준히 연습하고 있어 정말 멋져요!");
    }

    @DisplayName("잘못된 피드백 형식에 InvalidOpenAIResponseException을 던진다")
    @Test
    void generateSummaryAndFeedback_invalidFormat_shouldThrowException() {
        // given: 피드백 항목이 빠진 응답
        String invalidResponse = """
            {
              "choices": [
                {
                  "message": {
                    "content": "요약: 응답 형식이 잘못되었습니다."
                  }
                }
              ]
            }
            """;

        mockWebServer.enqueue(new MockResponse()
                .setResponseCode(200)
                .setBody(invalidResponse)
                .addHeader("Content-Type", "application/json"));

        StudyLog log = new StudyLog("오류 발생 가능성 테스트", LocalDateTime.now(), new User("U123", "Jia"));

        // when & then
        assertThrows(InvalidOpenAIResponseException.class, () ->
                        openAIClient.generateSummaryAndFeedback(List.of(log)),
                "응답에 '피드백:'이 빠졌기 때문에 예외가 발생해야 한다");

    }
}