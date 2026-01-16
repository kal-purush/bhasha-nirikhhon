package ru.otus.spring.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import java.util.Locale;

@Configuration
public class ApplicationProperties {

    @Value("${application.data.load.source}")
    private String dataLoadSource;
    @Value("${application.data.parse.delimiter}")
    private String delimiter;
    @Value("${application.data.parse.answerdelimiter}")
    private String answerDelimiter;
    @Value("${application.test.score}")
    private int maxScore;
    @Value("${application.test.passed}")
    private int minPassedPercent;
    @Value("${application.test.good}")
    private int minGoodPercent;
    @Value("${application.locale}")
    private Locale locale;

    public String getDataLoadSource() {
        return dataLoadSource;
    }

    public String getDelimiter() {
        return delimiter;
    }

    public String getAnswerDelimiter() {
        return answerDelimiter;
    }

    public int getMaxScore() {
        return maxScore;
    }

    public int getMinPassedPercent() {
        return minPassedPercent;
    }

    public int getMinGoodPercent() {
        return minGoodPercent;
    }

    public Locale getLocale() {
        return locale;
    }
}