package ru.otus.hw02.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

import java.util.Locale;

@Component
@PropertySource("classpath:application.properties")
public class LocalizationConfig {

    private final Locale locale;
    private final String questionsPath;
    private final String basenames;
    private final String defaultEncoding;

    public LocalizationConfig(@Value("${locale.language}") Locale locale,
                              @Value("${questions.path}") String questionsPath,
                              @Value("${locale.basenames}") String basenames,
                              @Value("${locale.encoding.default}") String defaultEncoding) {
        this.locale = locale;
        this.questionsPath = String.format(questionsPath, locale);
        this.basenames = basenames;
        this.defaultEncoding = defaultEncoding;
    }

    public Locale getLocale() {
        return locale;
    }

    public String getQuestionsPath() {
        return questionsPath;
    }

    public String getBasenames() {
        return basenames;
    }

    public String getDefaultEncoding() {
        return defaultEncoding;
    }
}