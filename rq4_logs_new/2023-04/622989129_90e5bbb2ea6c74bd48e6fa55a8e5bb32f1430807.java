package com.todolist.models;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class AuditDates {
    private DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
    private LocalDateTime now = LocalDateTime.now();
    private String createdAt = dtf.format(now);
    private String lastUpdatedAt = dtf.format(now);

    public AuditDates(String createdAt, String lastUpdatedAt){
        this.createdAt = createdAt;
        this.lastUpdatedAt = lastUpdatedAt;
    }
    public void setCreatedAt(String createdAt){ this.createdAt = createdAt; }
    public String getCreatedAt() { return createdAt; }

    public void setLastUpdatedAt(String lastUpdatedAt) {this.lastUpdatedAt = lastUpdatedAt; }
    public String getLastUpdatedAt() { return lastUpdatedAt; }

}