package com.eng.repository;

import com.eng.domain.Quiz;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
@RequiredArgsConstructor
public class JdbcRepository {
    private final JdbcTemplate template;

    public void batchInsert(List<Quiz> quizList) {
        // 1. Insert SQL 정의
        String sql = "INSERT INTO quiz (study_id, correct) VALUES (?, ?)";

        // 2. Batch Insert 실행 (sql, batchArgs, batchSize, sql ?에 들어갈 값)
        template.batchUpdate(sql, quizList, quizList.size(), (ps, quiz) -> {

            // PreparedStatement의 각 파라미터 설정
            ps.setLong(1, quiz.getStudy().getId());   // Study의 ID 설정
            ps.setBoolean(2, quiz.isCorrect());       // Correct 상태 설정
        });
    }
}