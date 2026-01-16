/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package Entity;

/**
 *
 * @author HP
 */
public class Question {
    private int qestId;
    private int quizId;
    private String questContent;
    private String optionA;
    private boolean answerA;
    private String optionB;
     private boolean answerB;
    private String optionC;
     private boolean answerC;
    private String optionD;
     private boolean answerD;
     

    public Question() {
    }

    public Question(int qestId, int quizId, String questContent, String optionA, boolean answerA, String optionB, boolean answerB, String optionC, boolean answerC, String optionD, boolean answerD) {
        this.qestId = qestId;
        this.quizId = quizId;
        this.questContent = questContent;
        this.optionA = optionA;
        this.answerA = answerA;
        this.optionB = optionB;
        this.answerB = answerB;
        this.optionC = optionC;
        this.answerC = answerC;
        this.optionD = optionD;
        this.answerD = answerD;
    }

    public int getQestId() {
        return qestId;
    }

    public void setQestId(int qestId) {
        this.qestId = qestId;
    }

    public int getQuizId() {
        return quizId;
    }

    public void setQuizId(int quizId) {
        this.quizId = quizId;
    }

    public String getQuestContent() {
        return questContent;
    }

    public void setQuestContent(String questContent) {
        this.questContent = questContent;
    }

    public String getOptionA() {
        return optionA;
    }

    public void setOptionA(String optionA) {
        this.optionA = optionA;
    }

    public boolean isAnswerA() {
        return answerA;
    }

    public void setAnswerA(boolean answerA) {
        this.answerA = answerA;
    }

    public String getOptionB() {
        return optionB;
    }

    public void setOptionB(String optionB) {
        this.optionB = optionB;
    }

    public boolean isAnswerB() {
        return answerB;
    }

    public void setAnswerB(boolean answerB) {
        this.answerB = answerB;
    }

    public String getOptionC() {
        return optionC;
    }

    public void setOptionC(String optionC) {
        this.optionC = optionC;
    }

    public boolean isAnswerC() {
        return answerC;
    }

    public void setAnswerC(boolean answerC) {
        this.answerC = answerC;
    }

    public String getOptionD() {
        return optionD;
    }

    public void setOptionD(String optionD) {
        this.optionD = optionD;
    }

    public boolean isAnswerD() {
        return answerD;
    }

    public void setAnswerD(boolean answerD) {
        this.answerD = answerD;
    }

    @Override
    public String toString() {
        return "Question{" + "qestId=" + qestId + ", quizId=" + quizId + ", questContent=" + questContent + ", optionA=" + optionA + ", answerA=" + answerA + ", optionB=" + optionB + ", answerB=" + answerB + ", optionC=" + optionC + ", answerC=" + answerC + ", optionD=" + optionD + ", answerD=" + answerD + '}';
    }
    
     
    
}