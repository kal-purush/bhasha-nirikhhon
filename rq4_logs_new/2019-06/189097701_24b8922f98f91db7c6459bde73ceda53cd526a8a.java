package ru.fmtk.hlystov.examinationapp;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import ru.fmtk.hlystov.examinationapp.domain.examination.Exam;
import ru.fmtk.hlystov.examinationapp.domain.statistics.ExamStatisticsImpl;
import ru.fmtk.hlystov.examinationapp.services.LocalizationService;
import ru.fmtk.hlystov.examinationapp.services.converter.ExamCSVFactory;
import ru.fmtk.hlystov.examinationapp.services.examinator.Examinator;
import ru.fmtk.hlystov.examinationapp.services.examinator.ExaminatorImpl;
import ru.fmtk.hlystov.examinationapp.services.presenter.ConsolePresenter;
import ru.fmtk.hlystov.examinationapp.services.presenter.Presenter;


public class Main {
    @NotNull
    private static final String SPRING_CONTEXT_RESOURCE_NAME = "/spring-context.xml";
    @Nullable
    private static ClassPathXmlApplicationContext springContext;
    @Nullable
    private static LocalizationService localizationService;

    public static void main(String[] args) {
        Presenter presenter = getSpringContext().getBean(ConsolePresenter.class);
        ExamCSVFactory examFactory = getSpringContext().getBean(ExamCSVFactory.class);
        Exam exam = examFactory.createExam();
        ExamStatisticsImpl statistics = getSpringContext().getBean(ExamStatisticsImpl.class);
        Examinator examinator = new ExaminatorImpl(presenter, exam, statistics);
        examinator.performExam();
    }

    @NotNull
    public static synchronized ClassPathXmlApplicationContext getSpringContext() {
        if (springContext == null) {
            springContext = new ClassPathXmlApplicationContext(SPRING_CONTEXT_RESOURCE_NAME);
        }
        return springContext;
    }

    @NotNull
    public static synchronized LocalizationService getLocalizationService() {
        if (localizationService == null) {
            localizationService = getSpringContext().getBean(LocalizationService.class);
        }
        return localizationService;
    }
}