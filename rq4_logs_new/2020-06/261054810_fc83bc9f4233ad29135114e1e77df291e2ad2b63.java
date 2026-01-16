package br.com.codenation.v1.errorManager.dto;

import br.com.codenation.v1.errorManager.enums.Level;

public class LogInfoDTO {
    private Long id;
    private String description;
    private String details;
    private String log;
    private String environment;
    private Integer level;
    private ApplicationDTO application;
    private String createdAt;
    private String updatedAt;
    private Long numberEvents;

    public LogInfoDTO(){
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
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

    public Level getLevel() {
        return Level.toEnum(level);
    }

    public void setLevel(Level level) {
        this.level = level.getCode();
    }

    public ApplicationDTO getApplication() {
        return application;
    }

    public void setApplication(ApplicationDTO application) {
        this.application = application;
    }

    public String getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(String createdAt) {
        this.createdAt = createdAt;
    }

    public Long getNumberEvents() {
        return numberEvents;
    }

    public void setNumberEvents(Long numberEvents) {
        this.numberEvents = numberEvents;
    }

    public String getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(String updatedAt) {
        this.updatedAt = updatedAt;
    }
}