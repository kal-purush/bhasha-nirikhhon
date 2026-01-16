package com.cruftlab.words;

import com.cruftlab.words.model.Word;
import com.cruftlab.words.repository.WordRepository;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class RandomWordsServiceRunner {
    @Bean
    public CommandLineRunner commandLineRunner(WordRepository wordRepository) {
        return args -> {
            wordRepository.save(new Word("test"));
            System.out.println(wordRepository.findAll());
        };
    }

    public static void main(String[] args) {
        SpringApplication.run(RandomWordsServiceRunner.class, args);
    }
}