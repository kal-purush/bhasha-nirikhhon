package com.roomfit.be.survey.domain;

import com.roomfit.be.global.entity.BaseEntity;
import jakarta.persistence.*;
import lombok.*;

import java.util.ArrayList;
import java.util.List;

/**
 * aggregation Root
 */
@Entity(name = "questionnaire")
@Builder
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class Questionnaire extends BaseEntity {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    Long id;
    String title;
    String description;

    @OneToMany(mappedBy = "questionnaire",cascade = CascadeType.ALL, orphanRemoval = true, fetch = FetchType.LAZY)
    @Builder.Default
    List<Question> questions = new ArrayList<>();
    public static Questionnaire create(String title, String description, List<Question> questions){
        Questionnaire questionnaire =  Questionnaire.builder()
                .title(title)
                .description(description)
                .build();

        questionnaire.initQuestions(questions);
        return questionnaire;
    }
    private void initQuestions(List<Question> questions){
        this.questions = questions;
        this.questions.forEach(question -> {
            question.addQuestionnaire(this);
        });
    }
}