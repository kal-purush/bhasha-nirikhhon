package com.jia.study_tracker.service;

import com.jia.study_tracker.domain.StudyLog;
import com.jia.study_tracker.domain.Summary;
import com.jia.study_tracker.domain.SummaryType;
import com.jia.study_tracker.domain.User;
import com.jia.study_tracker.repository.SummaryRepository;
import com.jia.study_tracker.repository.UserRepository;
import com.jia.study_tracker.service.dto.SummaryResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.*;

/**
 * SummaryGenerationService 테스트
 *
 * 목표:
 * - generateSummaries 메서드가 상황별로 정상 동작하는지 검증한다.
 *
 * 테스트 시나리오:
 * 1. 학습 로그가 없는 사용자는 스킵하고 저장하지 않는다.
 * 2. 학습 로그가 있는 사용자는 GPT 호출을 통해 요약을 생성하고 저장한다.
 */
@ExtendWith(MockitoExtension.class)
class SummaryGenerationServiceTest {

    @Mock
    private UserRepository userRepository;

    @Mock
    private StudyLogQueryService studyLogQueryService;

    @Mock
    private OpenAIClient openAIClient;

    @Mock
    private SummaryRepository summaryRepository;

    @InjectMocks
    private SummaryGenerationService summaryGenerationService;

    private User user;
    private LocalDate date;
    private SummaryType type;

    @BeforeEach
    void setUp() {
        user = new User("U123456", "jia");
        date = LocalDate.of(2025, 5, 2);
        type = SummaryType.DAILY;
    }

    @Test
    @DisplayName("사용자에게 학습 로그가 없으면 요약을 건너뛴다")
    void shouldSkipSummaryWhenNoLogsExist() {
        // given
        given(userRepository.findAll()).willReturn(List.of(user));
        given(studyLogQueryService.getLogs(user.getSlackUserId(), date, type)).willReturn(List.of());

        // when
        summaryGenerationService.generateSummaries(date, type);

        // then
        verify(openAIClient, never()).generateSummaryAndFeedback(any());
        verify(summaryRepository, never()).save(any());
    }

    @Test
    @DisplayName("학습 로그가 존재하면 GPT를 호출하고 결과를 저장한다")
    void shouldGenerateAndSaveSummaryWhenLogsExist() {
        // given
        List<StudyLog> logs = List.of(new StudyLog("공부 내용", LocalDateTime.now(), user));
        SummaryResult result = new SummaryResult("요약", "피드백");

        given(userRepository.findAll()).willReturn(List.of(user));
        given(studyLogQueryService.getLogs(user.getSlackUserId(), date, type)).willReturn(logs);
        given(openAIClient.generateSummaryAndFeedback(logs)).willReturn(result);

        // when
        summaryGenerationService.generateSummaries(date, type);

        // then
        verify(openAIClient).generateSummaryAndFeedback(logs);
        verify(summaryRepository).save(any(Summary.class));
    }
}