package spring.app.dto;

/**
 * Класс дто для хранения номера вопроса по теме и вычисленного айдишника следующего отвечающего
 * @author AkiraRokudo on 01.06.2020 in one of sun day
 */
public class QuestionNumberAnswererIdDto {

    private Integer questionNumber;
    private Long answererId;

    public QuestionNumberAnswererIdDto() {
    }

    public QuestionNumberAnswererIdDto(Integer questionNumber, Long answererId) {
        this.questionNumber = questionNumber;
        this.answererId = answererId;
    }

    public Integer getQuestionNumber() {
        return questionNumber;
    }

    public void setQuestionNumber(Integer questionNumber) {
        this.questionNumber = questionNumber;
    }

    public Long getAnswererId() {
        return answererId;
    }

    public void setAnswererId(Long answererId) {
        this.answererId = answererId;
    }
}