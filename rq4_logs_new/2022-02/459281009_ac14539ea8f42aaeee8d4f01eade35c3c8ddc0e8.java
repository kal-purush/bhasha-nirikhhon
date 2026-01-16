package ru.hogwarts.school.configuration;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.hogwarts.school.service.StudentService;

@Configuration
public class DefaultConfig {

    @Bean
    public StudentService studentService() {
        return new StudentService();
    }
}