package com.numo.api.domain.quiz.repository.query;

import com.numo.api.domain.quiz.dto.QuizStatResponseDto;
import com.numo.api.global.comm.date.DateCondition;
import com.numo.api.global.comm.date.DateRequestDto;
import com.numo.domain.quiz.QQuizStat;
import com.querydsl.core.types.Projections;
import com.querydsl.jpa.impl.JPAQueryFactory;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
@RequiredArgsConstructor
public class QuizStatQueryRepository {
    private final JPAQueryFactory jpaQueryFactory;

    QQuizStat qQuizStat = QQuizStat.quizStat;

    public List<QuizStatResponseDto> findQuizStatList(Long userId, DateRequestDto dateRequest) {
        return jpaQueryFactory.select(Projections.constructor(
                        QuizStatResponseDto.class,
                        qQuizStat.id,
                        qQuizStat.totalCount,
                        qQuizStat.correctCount,
                        qQuizStat.wrongCount,
                        qQuizStat.createTime,
                        qQuizStat.updateTime
                )).from(qQuizStat)
                .where(
                        qQuizStat.user.userId.eq(userId),
                        DateCondition.createDateCondition(qQuizStat.baseDate, dateRequest)
                ).fetch();
    }

}