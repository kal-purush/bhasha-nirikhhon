package ru.otus.hw01.service.validator;

import ru.otus.hw01.domain.question.Question;
import ru.otus.hw01.exception.DataException;
import ru.otus.hw01.exception.ModuleException;

public class DefaultQuestionValidator implements QuestionValidator {

    @Override
    public void validate(Question obj) throws ModuleException {
        if (obj.getId() < 1) {
            throw new DataException("Id", obj.getId());
        }
        if (obj.getQuestion() == null || obj.getQuestion().isEmpty()) {
            throw new DataException("Question", obj.getQuestion());
        }
        if (obj.getType() == null) {
            throw new DataException("Type", obj.getType());
        }
    }
}