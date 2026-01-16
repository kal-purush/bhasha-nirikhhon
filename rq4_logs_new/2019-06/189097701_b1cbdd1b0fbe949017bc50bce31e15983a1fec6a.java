package ru.fmtk.hlystov.examinationapp.domain.examination;

import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;
import ru.fmtk.hlystov.examinationapp.domain.statistics.ExamStatisticsImpl;
import ru.fmtk.hlystov.examinationapp.services.AppConfig;
import ru.fmtk.hlystov.examinationapp.services.converter.QuestionsCSVLoader;
import ru.fmtk.hlystov.examinationapp.services.examinator.Examinator;
import ru.fmtk.hlystov.examinationapp.services.examinator.ExaminatorImpl;
import ru.fmtk.hlystov.examinationapp.services.presenter.ConsolePresenter;
import ru.fmtk.hlystov.examinationapp.services.presenter.Presenter;

import java.io.IOException;
import java.io.InputStream;

@Component
public class ExamFactory {
    private static ApplicationContext springContext;
    private static AppConfig appConfig;
    QuestionsCSVLoader questionsCSVLoader;

    @Autowired
    public void setSpringContext(ApplicationContext springContext) {
        this.springContext = springContext;
    }

    @Autowired
    public static void setAppConfig(AppConfig appConfig) {
        ExamFactory.appConfig = appConfig;
    }

    @Autowired
    public void setQuestionsCSVLoader(QuestionsCSVLoader questionsCSVLoader) {
        this.questionsCSVLoader = questionsCSVLoader;
    }

    public Exam createExam() {
        Examinator examinator = springContext.getBean(Examinator.class);
        Exam exam = examinator.getExam();
        appConfig.getSCVQuestionsStream()
                .ifPresent(is -> fillExamByQuestionsStream(exam, is));
        examinator.performExam();
        return exam;
    }

    private void fillExamByQuestionsStream(@NotNull Exam exam,
                                           @NotNull InputStream inputStream) {
        try {
            exam.addQuestions(questionsCSVLoader.readQuestions(inputStream));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}