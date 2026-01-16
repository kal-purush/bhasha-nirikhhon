package spring.app.listener;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import spring.app.model.Question;
import spring.app.model.StudentReviewAnswer;
import spring.app.service.abstraction.StudentReviewAnswerService;

import javax.persistence.PreRemove;
import java.util.List;


@Component
public class QuestionListener {
    private static StudentReviewAnswerService studentReviewAnswerService;

    @Autowired
    public void setStudentReviewAnswerService(StudentReviewAnswerService studentReviewAnswerService) {
        QuestionListener.studentReviewAnswerService = studentReviewAnswerService;
    }

    @PreRemove
    private void removeRelatedEntities(Question question) {
        List<StudentReviewAnswer> studentReviewAnswersByQuestionId = studentReviewAnswerService.getStudentReviewAnswersByQuestionId(question.getId());
        studentReviewAnswerService.removeAll(studentReviewAnswersByQuestionId);
    }
}