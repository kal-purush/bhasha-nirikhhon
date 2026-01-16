package com.example.study;

public class Question {

    private int id;
    private String question;
    private String optionOne;
    private String optionTwo;
    private String optionThree;
    private String optionFour;
    private String correctOption;

    public Question() {
    }

    public Question(int id, String question, String optionOne, String optionTwo, String optionThree, String optionFour, String correctOption) {
        this.id = id;
        this.question = question;
        this.optionOne = optionOne;
        this.optionTwo = optionTwo;
        this.optionThree = optionThree;
        this.optionFour = optionFour;
        this.correctOption = correctOption;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getQuestion() {
        return question;
    }
    public String getCorrectOption(){
        return correctOption;
    }
    public String getOptionOne() {
        return optionOne;
    }
    public String getOptionTwo() {
        return optionTwo;
    }
    public String getOptionThree() {
        return optionThree;
    }
    public String getOptionFour() {
        return optionFour;
    }

}