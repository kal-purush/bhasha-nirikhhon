package ru.otus.hw01.service.validator;

import ru.otus.hw01.domain.question.Question;
import ru.otus.hw01.exception.DataException;

public class QuestionValidator {

    public static void validate(Question question) throws DataException {
        if (question.getId() < 1) {
            throw new DataException("Id", question.getId());
        }
        if (question.getQuestion() == null || question.getQuestion().isEmpty()) {
            throw new DataException("Question", question.getQuestion());
        }
        if (question.getType() == null) {
            throw new DataException("Type", question.getType());
        }
    }
}