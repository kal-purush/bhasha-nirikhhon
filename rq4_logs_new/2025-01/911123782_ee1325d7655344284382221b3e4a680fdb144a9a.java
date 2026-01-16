package com.roomfit.be.survey.infrastructure;

import com.roomfit.be.survey.domain.Question;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;

public interface QuestionRepository extends JpaRepository<Question, Long> {
    @Query("select q from questions q " +
            "join fetch q.replies r " +
            "where r.owner.id = :ownerId and q.questionnaire.id = :questionnaireId")
    List<Question> findByOwnerId(@Param("ownerId") Long ownerId, @Param("questionnaireId") Long questionnaireId);
}