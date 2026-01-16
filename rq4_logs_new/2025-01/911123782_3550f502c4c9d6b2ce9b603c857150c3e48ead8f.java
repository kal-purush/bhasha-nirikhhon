package com.roomfit.be.survey.presentation.aspect;

import com.roomfit.be.auth.application.dto.UserDetails;
import com.roomfit.be.user.domain.SurveyStage;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

@Aspect
@Slf4j
@Component
@Order(2)
public class SurveyStageAspect {
    @Before("@annotation(com.roomfit.be.survey.presentation.annotation.SurveyStageCheck) && args(userDetails, ..)")
    public void checkSurveyStatus(UserDetails userDetails) {
        log.info(userDetails.getSurveyStage());
        if (SurveyStage.POST_SURVEY.name().equals(userDetails.getSurveyStage())) {
            throw new RuntimeException("현재 설문 단계에서 답변을 생성할 수 없습니다.");
        }
    }
}