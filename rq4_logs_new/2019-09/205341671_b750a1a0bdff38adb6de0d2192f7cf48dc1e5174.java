package ru.otus.hw02;

import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import ru.otus.hw02.exception.ModuleException;
import ru.otus.hw02.service.ExamService;

@Configuration
@ComponentScan
public class Main {

    public static void main(String[] args) throws ModuleException {
        try(AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(Main.class)) {
            ExamService examService = context.getBean(ExamService.class);
            examService.askQuestions();
        }
    }
}