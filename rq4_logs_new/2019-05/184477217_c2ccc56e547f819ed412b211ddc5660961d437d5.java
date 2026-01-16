package com.triviamachine.triviamachine.database;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.OneToMany;
import java.util.List;

@Entity
public class Question {
    @Id
    @GeneratedValue
    private long id;

    private String questionText;
    private String answerOne;
    private String answerTwo;
    private String answerThree;
    private String answerFour;

    @OneToMany(mappedBy = "question")
    private List<Results> questionResults;

    @OneToMany(mappedBy = "question")
    private List<QuestionSchedule> timesScheduled;

    public String getAnswerTwo() {
        return answerTwo;
    }

    public void setAnswerTwo(String answerTwo) {
        this.answerTwo = answerTwo;
    }

    public String getAnswerOne() {
        return answerOne;
    }

    public void setAnswerOne(String answerOne) {
        this.answerOne = answerOne;
    }

    public String getQuestionText() {
        return questionText;
    }

    public void setQuestionText(String questionText) {
        this.questionText = questionText;
    }

    public String getAnswerThree() {
        return answerThree;
    }

    public void setAnswerThree(String answerThree) {
        this.answerThree = answerThree;
    }

    public String getAnswerFour() {
        return answerFour;
    }

    public void setAnswerFour(String answerFour) {
        this.answerFour = answerFour;
    }

    public long getId() {
        return id;
    }

    public List<Results> getQuestionResults() {
        return questionResults;
    }

    public List<QuestionSchedule> getTimesScheduled() {
        return timesScheduled;
    }
}