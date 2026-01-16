package com.example.backend.dtos;

import com.example.backend.enums.TaskStatus;
import javax.validation.constraints.NotBlank;

import java.util.Date;
import java.util.UUID;

public class TaskDto {
    @NotBlank
    private String name;
    private TaskStatus status;
    private Date startDate;
    private Date endDate;
    private UUID userId;
    private UUID taskId;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public TaskStatus getStatus() {
        return status;
    }

    public void setStatus(TaskStatus status) {
        this.status = status;
    }

    public Date getStartDate() {
        return startDate;
    }

    public void setStartDate(Date startDate) {
        this.startDate = startDate;
    }

    public Date getEndDate() {
        return endDate;
    }

    public void setEndDate(Date endDate) {
        this.endDate = endDate;
    }

    public UUID getUserId() {
        return userId;
    }

    public void setUserId(UUID userId) {
        this.userId = userId;
    }

    public UUID getTaskId() {
        return taskId;
    }

    public void setTaskId(UUID taskId) {
        this.taskId = taskId;
    }
}

