package com.roomfit.be.survey.domain;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;

@Entity(name = "questions")
@Getter
@Setter
@NoArgsConstructor
public class Question {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    Long id;
    String title;
    @Enumerated(EnumType.STRING)
    QuestionType type;
    @Column(nullable = true)
    String optionDelimiter;

    @OneToMany(mappedBy = "question",cascade = CascadeType.PERSIST, orphanRemoval = true, fetch = FetchType.LAZY)
    List<Option> options;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "questionnaire_id")
    private Questionnaire questionnaire;
    public Question(String title, QuestionType type, String optionDelimiter, List<Option> options) {

        this.title = title;
        this.type = type;
        this.optionDelimiter = optionDelimiter;
        this.options = options;
    }
    public void addQuestionnaire(Questionnaire questionnaire) {
        this.questionnaire = questionnaire;
    }
    public static Question createSlider(String title, String optionDelimiter, List<Option> options) {
        return createQuestionWithOptions(title, QuestionType.SLIDER, optionDelimiter, options);
    }

    public static Question createDoubleSlider(String title, String optionDelimiter, List<Option> options) {
        return createQuestionWithOptions(title, QuestionType.DOUBLE_SLIDER, optionDelimiter, options);
    }

    public static Question createCheckbox(String title, String optionDelimiter, List<Option> options) {
        return createQuestionWithOptions(title, QuestionType.CHECKBOX, optionDelimiter, options);
    }

    public static Question createSelector(String title, String optionDelimiter, List<Option> options) {
        return createQuestionWithOptions(title, QuestionType.SELECTOR, optionDelimiter, options);
    }

    private static Question createQuestionWithOptions(String title, QuestionType type, String optionDelimiter, List<Option> options) {
        Question question =  new Question(title, type, optionDelimiter, options);
        question.getOptions().forEach(option->option.setQuestion(question));
        return question;
    }
}

