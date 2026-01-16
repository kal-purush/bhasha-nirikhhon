package edu.asu.heal.reachv3.api.models;

import edu.asu.heal.core.api.models.ActivityInstance;
import edu.asu.heal.core.api.models.ActivityInstanceType;

public class FaceItModel {
    private int questionId;
    private String questionText;
    private int answerId;
    private String status;

    public FaceItModel() {

    }

    public FaceItModel(int questionId, String questionText, int answerId, String status) {
        this.questionId = questionId;
        this.questionText = questionText;
        this.answerId = answerId;
        this.status = status;
    }

    public String getQuestionText() {
        return questionText;
    }

    public void setQuestionText(String questionText) {
        this.questionText = questionText;
    }

    public int getAnswerId() {
        return answerId;
    }

    public void setAnswerId(int answerId) {
        this.answerId = answerId;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public int getQuestionId() {
        return questionId;
    }

    public void setQuestionId(int questionId) {
        this.questionId = questionId;
    }
}