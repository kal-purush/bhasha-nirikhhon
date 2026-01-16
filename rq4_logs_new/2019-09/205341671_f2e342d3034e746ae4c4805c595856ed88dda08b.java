package ru.otus.hw02;

import org.springframework.context.support.ClassPathXmlApplicationContext;
import ru.otus.hw02.exception.ModuleException;
import ru.otus.hw02.service.ExamService;

public class Main {

    public static void main(String[] args) throws ModuleException {
        try(ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext("/spring-context.xml")) {
            ExamService examService = context.getBean(ExamService.class);
            examService.askQuestions();
        }
    }
}