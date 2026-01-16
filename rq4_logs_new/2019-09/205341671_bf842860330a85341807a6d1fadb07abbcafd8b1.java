package ru.otus.hw01.service.statistic;

import javax.validation.constraints.Positive;
import javax.validation.constraints.PositiveOrZero;

public class DefaultStatistic implements Statistic {

    @Positive
    private final int count;
    @PositiveOrZero
    private int correctAnswers;

    DefaultStatistic(int count) {
        this.count = count;
    }

    @Override
    public void incrementCorrectAnswers() {
        correctAnswers++;
    }

    @Override
    public int getQuestionsCount() {
        return count;
    }

    @Override
    public int getCorrectAnswersCount() {
        return correctAnswers;
    }

    @Override
    public int getResultMark() {
        return correctAnswers*100/count; //Пусть округляет в меньшую сторону:)
    }
}