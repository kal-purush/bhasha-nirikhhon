package com.roomfit.be;

import com.roomfit.be.survey.application.SurveyService;
import com.roomfit.be.survey.application.dto.OptionDTO;
import com.roomfit.be.survey.application.dto.QuestionDTO;
import com.roomfit.be.survey.application.dto.QuestionnaireDTO;
import com.roomfit.be.survey.domain.Option;
import com.roomfit.be.survey.domain.Question;
import com.roomfit.be.survey.domain.Questionnaire;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.ArrayList;
import java.util.List;

@SpringBootTest
@Slf4j
public class SurveyTest {
    @Autowired
    private SurveyService surveyService;

    @Test
    void 설문지_생성() {
        List<Question> questions = List.of(
                Question.createSlider("질문 유형 1", null, DummyOptionFactory.create()),
                Question.createCheckbox("질문 유형 2", null, DummyOptionFactory.create())
        );
        Questionnaire questionnaire = Questionnaire.create("title", "des", questions);
        System.out.println(questionnaire);
    }

    @Test
    void 설문지_생성_서비스로() {
        List<QuestionDTO.Request> questions = List.of(
                new QuestionDTO.Request("질문1", "slider", null, DummyOptionDTOFactory.create()),
                new QuestionDTO.Request("질문2", "doubleSlider", null, DummyOptionDTOFactory.create()),
                new QuestionDTO.Request("질문3", "selector", null, DummyOptionDTOFactory.create())
        );
        long startTime = System.currentTimeMillis();
        QuestionnaireDTO.Create request = new QuestionnaireDTO.Create("title", "description", questions);
        long endTime = System.currentTimeMillis();
        log.info("Execution time: " + (endTime - startTime) + " milliseconds");
        QuestionnaireDTO.Response response = surveyService.createQuestionnaire(request);
        log.info(response.toString());
    }

    @Test
    void 설문지_생성_성능비교() {
        int dataCount = 23;
        int repeatCount = 2;
        List<Long> createQuestionnaireTimes = new ArrayList<>();
        List<Long> createQuestionnaireLegacyTimes = new ArrayList<>();

        for (int i = 0; i < repeatCount; i++) {
            List<QuestionDTO.Request> questions = createSurveyData(dataCount);

            QuestionnaireDTO.Create request = new QuestionnaireDTO.Create("title", "description", questions);

            // New implementation time measurement
            long startTime = System.currentTimeMillis();

            QuestionnaireDTO.Response legacyResponse = surveyService.createQuestionnaireLegacy(request);
            long endTime = System.currentTimeMillis();
            createQuestionnaireLegacyTimes.add(endTime - startTime);

            // Legacy implementation time measurement
            startTime = System.currentTimeMillis();
            QuestionnaireDTO.Response response = surveyService.createQuestionnaire(request);
            endTime = System.currentTimeMillis();
            createQuestionnaireTimes.add(endTime - startTime);
        }

        double createQuestionnaireAvgTime = createQuestionnaireTimes.stream()
                .mapToLong(Long::longValue).average().orElse(0);
        long createQuestionnaireMaxTime = createQuestionnaireTimes.stream()
                .mapToLong(Long::longValue).max().orElse(0);
        long createQuestionnaireMinTime = createQuestionnaireTimes.stream()
                .mapToLong(Long::longValue).min().orElse(0);

        double createQuestionnaireLegacyAvgTime = createQuestionnaireLegacyTimes.stream()
                .mapToLong(Long::longValue).average().orElse(0);
        long createQuestionnaireLegacyMaxTime = createQuestionnaireLegacyTimes.stream()
                .mapToLong(Long::longValue).max().orElse(0);
        long createQuestionnaireLegacyMinTime = createQuestionnaireLegacyTimes.stream()
                .mapToLong(Long::longValue).min().orElse(0);

        log.info("createQuestionnaire 평균 실행 시간: " + createQuestionnaireAvgTime + " ms");
        log.info("createQuestionnaire 최소 실행 시간: " + createQuestionnaireMinTime + " ms");
        log.info("createQuestionnaire 최대 실행 시간: " + createQuestionnaireMaxTime + " ms");

        log.info("createQuestionnaireLegacy 평균 실행 시간: " + createQuestionnaireLegacyAvgTime + " ms");
        log.info("createQuestionnaireLegacy 최소 실행 시간: " + createQuestionnaireLegacyMinTime + " ms");
        log.info("createQuestionnaireLegacy 최대 실행 시간: " + createQuestionnaireLegacyMaxTime + " ms");

        if (createQuestionnaireAvgTime < createQuestionnaireLegacyAvgTime) {
            log.info("createQuestionnaire가 더 빠릅니다.");
        } else if (createQuestionnaireAvgTime > createQuestionnaireLegacyAvgTime) {
            log.info("createQuestionnaireLegacy가 더 빠릅니다.");
        } else {
            log.info("두 함수의 실행 시간이 동일합니다.");
        }
    }

    private List<QuestionDTO.Request> createSurveyData(int dataCount) {
        List<QuestionDTO.Request> questions = new ArrayList<>();
        for (int i = 0; i < dataCount; i++) {
            questions.add(new QuestionDTO.Request("질문" + (i + 1), "slider", null, DummyOptionDTOFactory.create()));
        }
        return questions;
    }

    class DummyOptionDTOFactory {
        public static List<OptionDTO.Request> create() {
            return List.of(
                    new OptionDTO.Request("label", "value"),
                    new OptionDTO.Request("label", "value"),
                    new OptionDTO.Request("label", "value")
            );
        }
    }

    class DummyOptionFactory {
        public static List<Option> create() {
            return List.of(
                    new Option("min", "0"),
                    new Option("max", "10")
            );
        }
    }
}