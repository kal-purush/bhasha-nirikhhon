package com.roomfit.be.survey.infrastructure.support;

import com.roomfit.be.survey.domain.Question;
import com.roomfit.be.survey.domain.Questionnaire;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Repository;

import java.sql.PreparedStatement;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
@Repository
@RequiredArgsConstructor
public class QuestionnaireRepositoryImpl implements QuestionnaireRepository {
    private final JdbcTemplate jdbcTemplate;

    @Transactional
    @Override
    public void saveQuestionnaire(Questionnaire questionnaire) {
        if (questionnaire.getCreatedAt() == null) {
            questionnaire.setCreatedAt(LocalDateTime.now());
        }

        String questionnaireSql = "INSERT INTO questionnaire (title, description, created_at) VALUES (?, ?, ?)";
        KeyHolder keyHolder = new GeneratedKeyHolder();

        jdbcTemplate.update(connection -> {
            PreparedStatement ps = connection.prepareStatement(questionnaireSql, new String[]{"id"});
            ps.setString(1, questionnaire.getTitle());
            ps.setString(2, questionnaire.getDescription());
            ps.setObject(3, questionnaire.getCreatedAt());
            return ps;
        }, keyHolder);

        Long questionnaireId = keyHolder.getKey().longValue();
        questionnaire.setId(questionnaireId);

        saveQuestions(questionnaire.getQuestions(), questionnaireId);

        saveOptions(questionnaire.getQuestions());

    }

    private void saveQuestions(List<Question> questions, Long questionnaireId) {
        String questionSql = "INSERT INTO questions (title, type, option_delimiter, questionnaire_id) VALUES (?, ?, ?, ?)";

        List<Object[]> questionParams = new ArrayList<>();

        for (Question question : questions) {
            questionParams.add(new Object[]{
                    question.getTitle(),
                    question.getType().name(),
                    question.getOptionDelimiter(),
                    questionnaireId
            });
        }

        jdbcTemplate.batchUpdate(questionSql, questionParams);

        String getLastInsertedIdsSql = "SELECT id FROM questions WHERE questionnaire_id = ? ORDER BY id DESC LIMIT ?";
        List<Long> generatedQuestionIds = jdbcTemplate.queryForList(getLastInsertedIdsSql, Long.class, questionnaireId, questions.size());

        for (int i = 0; i < questions.size(); i++) {
            questions.get(i).setId(generatedQuestionIds.get(i));
        }
    }

    private void saveOptions(List<Question> questions) {
        String optionSql = "INSERT INTO options (option_label, option_value, question_id) VALUES (?, ?, ?)";

        List<Object[]> optionParams = new ArrayList<>();

        for (Question question : questions) {
            Long questionId = question.getId();
            if (questionId != null) {
                question.getOptions().forEach(option -> {
                    optionParams.add(new Object[]{
                            option.getLabel(),
                            option.getValue(),
                            questionId
                    });
                });
            }
        }

        // Batch insert options
        jdbcTemplate.batchUpdate(optionSql, optionParams);
    }
}