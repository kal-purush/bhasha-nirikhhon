package ru.otus.spring.web_ui_book_info_app.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix="spring.shell.output")
public class ShellOutputConfig {
    private String errorMessage;
    private String notFoundMessage;
    private String defaultMessage;

    public String getNotFoundMessage() {
        return notFoundMessage;
    }

    public void setNotFoundMessage(String notFoundMessage) {
        this.notFoundMessage = notFoundMessage;
    }

    public String getDefaultMessage() {
        return defaultMessage;
    }

    public void setDefaultMessage(String defaultMessage) {
        this.defaultMessage = defaultMessage;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }
}