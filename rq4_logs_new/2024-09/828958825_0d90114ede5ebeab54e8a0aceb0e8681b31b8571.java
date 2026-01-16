package org.example.qint_backend.domain.question.service;

import lombok.RequiredArgsConstructor;
import org.example.qint_backend.domain.question.domain.Question;
import org.example.qint_backend.domain.question.domain.UserIncorrectAnswers;
import org.example.qint_backend.domain.question.domain.repository.AnswerRepository;
import org.example.qint_backend.domain.question.domain.repository.QuestionRepository;
import org.example.qint_backend.domain.question.domain.repository.UserIncorrectAnswersRepository;
import org.example.qint_backend.domain.question.presentation.dto.request.CategoryRequest;
import org.example.qint_backend.domain.question.presentation.dto.response.OptionsElement;
import org.example.qint_backend.domain.question.presentation.dto.response.QuestionByCategoryElement;
import org.example.qint_backend.domain.question.presentation.dto.response.QuestionByCategoryResponse;
import org.example.qint_backend.domain.user.domain.User;
import org.example.qint_backend.domain.user.facade.UserFacade;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;


@Service
@RequiredArgsConstructor
public class GetQuestionByCategoryService {

    private final UserIncorrectAnswersRepository userIncorrectAnswersRepository;

    private final QuestionRepository questionRepository;

    private final AnswerRepository answerRepository;

    private final UserFacade userFacade;

    public QuestionByCategoryResponse execute(CategoryRequest categoryRequest) {
        User user = userFacade.getCurrentUser();

        List<String> categorys = categoryRequest.getCategorys();


        List<Question> incorrectQuestions = null;
        for (String category : categorys) {
            incorrectQuestions.addAll(userIncorrectAnswersRepository.findAllByQuestionCategoryName(category)
                    .stream().map(UserIncorrectAnswers::getQuestion).toList());
        }
        Collections.shuffle(incorrectQuestions);

        List<Question> questions = null;
        for (String category : categorys) {
            questions.addAll(questionRepository.findAllByCategoryName(category));
        }
        questions.removeAll(incorrectQuestions);
        Collections.shuffle(questions);

        incorrectQuestions.addAll(questions);

        incorrectQuestions = incorrectQuestions.subList(0, 15);

        Collections.shuffle(incorrectQuestions);

        return QuestionByCategoryResponse.builder().questions(
                incorrectQuestions.stream()
                        .map(question -> QuestionByCategoryElement.builder()
                                .question_Id(question.getId())
                                .options(answerRepository.findAllByQuestion(question).stream()
                                        .map(a -> OptionsElement.builder()
                                                .answerId(a.getId())
                                                .text(a.getText()).build())
                                        .toList())
                                .contents(question.getContents()).build())
                        .toList())
                .build();

    }
}