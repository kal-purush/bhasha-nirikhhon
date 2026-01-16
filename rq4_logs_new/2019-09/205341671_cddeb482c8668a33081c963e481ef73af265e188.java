package ru.otus.hw01.domain.question;

import org.springframework.lang.NonNull;

import javax.validation.constraints.NotEmpty;

public class Question {

    @NonNull
    private final long id;
    @NonNull
    @NotEmpty
    private final String question;
    private final String answer;
    @NonNull
    @NotEmpty
    private final QuestionType type;

    public Question(long id, String question, String answer, QuestionType type) {
        this.id = id;
        this.question = question;
        this.answer = answer;
        this.type = type;
    }

    public Question(long id, String question, String answer, String type) {
        this.id = id;
        this.question = question;
        this.answer = answer;
        this.type = QuestionType.valueOf(type);
    }

    public long getId() {
        return id;
    }

    public String getQuestion() {
        return question;
    }

    public String getAnswer() {
        return answer;
    }

    public QuestionType getType() {
        return type;
    }
}