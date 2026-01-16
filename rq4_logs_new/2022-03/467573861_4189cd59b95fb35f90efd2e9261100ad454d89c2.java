package ru.platon.bot2.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.platon.bot2.entities.questionnaire.Answer;
import ru.platon.bot2.entities.questionnaire.Question;
import ru.platon.bot2.entities.questionnaire.Questionnaire;
import ru.platon.bot2.repository.AnswerRepository;
import ru.platon.bot2.repository.CompletedQuestionnaireRepository;
import ru.platon.bot2.repository.QuestionRepository;
import ru.platon.bot2.repository.QuestionnaireRepository;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Service
@Slf4j
public class FillingBD {

    private final AnswerRepository answerRepository;
    private final CompletedQuestionnaireRepository cQRepository;
    private final QuestionnaireRepository questionnaireRep;
    private final QuestionRepository questionRepository;

    public FillingBD(AnswerRepository answerRepository, 
                     CompletedQuestionnaireRepository cQRepository, 
                     QuestionnaireRepository qRepository, 
                     QuestionRepository questionRepository) {
        this.answerRepository = answerRepository;
        this.cQRepository = cQRepository;
        this.questionnaireRep = qRepository;
        this.questionRepository = questionRepository;
    }

    public void fillingAllBD(){
        fillingAnswer();
        fillingQuestion();
    }

    /**
     * Метод вставляет необходимых юзеров
     */
    public void fillingUser(){

    }
    /**
     * Метод вставляет все ответы
     */
    public void fillingAnswer(){
        answerRepository.saveAllAndFlush(List.of(
                new Answer(1L,"Рим"),new Answer(2L,"Москва"),new Answer(3L,"Казань"),
                new Answer(4L,"Венеция"),

                new Answer(5L,"Москвич"),new Answer(6L,"Феррари"),new Answer(7L,"Лендровер"),
                new Answer(8L,"KIA"),

                new Answer(9L,"92"),new Answer(10L,"96"),new Answer(11L,"100"),
                new Answer(12L,"Ракетное топливо"),

                new Answer(13L,"60-80 км/ч"),new Answer(14L,"80-100 км/ч"),
                new Answer(15L,"120-140 км/ч"),new Answer(16L,"140-200 км/ч")
        ));
    }

    /**
     * Метод вставляет все вопросы
     */
    public void fillingQuestion(){
        questionRepository.saveAllAndFlush(List.of(
                new Question(1L, "Какой город Вы бы выбрали?"),
                new Question(2L, "Какую машину Вы бы выбрали?"),
                new Question(3L, "Какой бензин Вы бы выбрали?"),
                new Question(4L, "Какую скорость Вы бы выбрали?")
        ));
    }

    /**
     * Метод вставляет все опросники
     */
    public void fillingQuestionnaire(){
        Map<Long, String> mapQuestionAndAnswers = new LinkedHashMap<>();
        mapQuestionAndAnswers.put(1L,"1,2,3,4");
        mapQuestionAndAnswers.put(2L,"5,6,7,8");
        mapQuestionAndAnswers.put(3L,"9,10,11,12");
        mapQuestionAndAnswers.put(4L,"13,14,15,16");
        questionnaireRep.saveAllAndFlush(List.of(
                new Questionnaire(1L,"Начальный опросник", mapQuestionAndAnswers)
        ));
    }
}