package ru.otus.homework4.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "application")
public class AppConfig {

    private String locale;
    private String fileName;

    public String getLocale() {
        return locale;
    }

    public void setLocale(String locale) {
        this.locale = locale;
    }

    public String getFileName() {
        return fileName + "_" + locale + ".csv";
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }
}