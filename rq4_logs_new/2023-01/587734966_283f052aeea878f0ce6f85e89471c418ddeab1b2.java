import pl.nullpointerexception.restapp.Answer.Answer;
import pl.nullpointerexception.restapp.Answer.NewAnswerDto;
import pl.nullpointerexception.restapp.Question.NewQuestionDto;
import pl.nullpointerexception.restapp.Question.Question;

public class klasa {

    NewAnswerDto getAnswerByQuestionID(Long questionId) {
        Answer answer = new Answer();
        Question question = new Question();
        question.setId(questionId);
       // answer = answerRepository.save(answer);
        answer.setId(answer.getId());
        question.addAnswer(answer);
       // question = questionRepository.save(question);
        NewQuestionDto questionDto = new NewQuestionDto();
        NewAnswerDto answerDto = new NewAnswerDto();
       // answerDto.setId(answerId);
        questionDto.getQuestionById(questionId);
        questionDto.getAnswers();
        return answerDto;
    }


}