package com.jia.study_tracker.service;

import com.jia.study_tracker.domain.StudyLog;
import com.jia.study_tracker.domain.SummaryType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.jdbc.Sql;

import java.time.LocalDate;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * StudyLogQueryService 테스트
 *
 * 목표:
 * - 슬랙 유저 ID, 날짜, 요약 타입(SummaryType: DAILY, WEEKLY, MONTHLY)을 기반으로
 *   StudyLog를 조회하는지 검증한다.
 * - 존재하지 않는 사용자에 대해 예외 처리가 동작하는지를 확인한다.
 *
 * 테스트 시나리오:
 * 1. DAILY, WEEKLY, MONTHLY 타입에 맞는 StudyLog만 반환해야 한다.
 * 2. 유저별로 각자의 StudyLog만 반환해야 한다.
 * 4. 존재하지 않는 사용자 조회 시, IllegalArgumentException을 던져야 한다.
 *
 * 사용 데이터:
 * - @Sql("/data.sql")로 고정된 테스트 데이터를 삽입한다.
 */
@SpringBootTest
@Sql(scripts = "/data.sql")
class StudyLogQueryServiceTest {

    @Autowired
    private StudyLogQueryService studyLogQueryService;

    @DisplayName("DAILY 타입으로 StudyLog를 조회한다")
    @Test
    void getDailyLogs() {
        // given
        String slackUserIdJia = "U123456";
        LocalDate baseDate = LocalDate.of(2025, 5, 2);

        // when
        List<StudyLog> logs = studyLogQueryService.getLogs(slackUserIdJia, baseDate, SummaryType.DAILY);

        // then
        assertThat(logs).hasSize(1);
        assertThat(logs)
                .extracting("content")
                .containsExactlyInAnyOrder(
                        "스프링 JPA 복습"
                );
    }

    @DisplayName("WEEKLY 타입으로 StudyLog를 조회한다")
    @Test
    void getWeeklyLogs() {
        // given
        String slackUserIdJia = "U123456";
        LocalDate baseDate = LocalDate.of(2025, 4, 28); // 월요일로 가정

        // when
        List<StudyLog> logs = studyLogQueryService.getLogs(slackUserIdJia, baseDate, SummaryType.WEEKLY);

        // then
        assertThat(logs).hasSize(3);
        assertThat(logs)
                .extracting("content")
                .containsExactlyInAnyOrder(
                        "오늘 자바 공부함",
                        "스프링 JPA 복습",
                        "OpenAI 연동 테스트"
                );
    }

    @DisplayName("MONTHLY 타입으로 StudyLog를 조회한다")
    @Test
    void getMonthlyLogs() {
        // given
        String slackUserIdJia = "U123456";
        LocalDate baseDate = LocalDate.of(2025, 5, 1);

        // when
        List<StudyLog> logs = studyLogQueryService.getLogs(slackUserIdJia, baseDate, SummaryType.MONTHLY);

        // then
        assertThat(logs).hasSize(3);
        assertThat(logs)
                .extracting("content")
                .containsExactlyInAnyOrder(
                        "오늘 자바 공부함",
                        "스프링 JPA 복습",
                        "OpenAI 연동 테스트"
                );
    }

    @DisplayName("유저별로 로그를 조회한다")
    @Test
    void getLogsShouldReturnOnlyLogsOfSpecificUser() {
        // given
        String slackUserIdJia = "U123456";
        String slackUserIdOther = "U999999";
        LocalDate baseDate = LocalDate.of(2025, 5, 2); // DAILY

        // when
        List<StudyLog> logsForJia = studyLogQueryService.getLogs(slackUserIdJia, baseDate, SummaryType.DAILY);
        List<StudyLog> logsForOther = studyLogQueryService.getLogs(slackUserIdOther, baseDate, SummaryType.DAILY);

        // then
        assertThat(logsForJia).extracting("content")
                .containsExactlyInAnyOrder("스프링 JPA 복습");

        assertThat(logsForOther).extracting("content")
                .containsExactlyInAnyOrder("영어를 공부함");
    }


    @DisplayName("존재하지 않는 유저는 예외를 던진다")
    @Test
    void throwsExceptionIfUserNotFound() {
        // given
        String invalidUserId = "UNKNOWN";
        LocalDate baseDate = LocalDate.of(2025, 5, 2);

        // when & then
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () ->
                studyLogQueryService.getLogs(invalidUserId, baseDate, SummaryType.DAILY));
    }
}