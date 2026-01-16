package br.com.codenation.v1.errorManager.dto;

public class LogDTO {

    private String description;
    private String details;
    private String log;
    private String environment;
    private Integer level;
    private String application;

    public LogDTO(){

    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getDetails() {
        return details;
    }

    public void setDetails(String details) {
        this.details = details;
    }

    public String getLog() {
        return log;
    }

    public void setLog(String log) {
        this.log = log;
    }

    public String getEnvironment() {
        return environment;
    }

    public void setEnvironment(String environment) {
        this.environment = environment;
    }

    public Integer getLevel() {
        return level;
    }

    public void setLevel(Integer level) {
        this.level = level;
    }

    public String getapplication() {
        return application;
    }

    public void setApplicationName(String application) {
        this.application = application;
    }
}