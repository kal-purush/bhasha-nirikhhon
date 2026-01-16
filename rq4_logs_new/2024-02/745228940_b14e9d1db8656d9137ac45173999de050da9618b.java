package org.example.JavaTaigaCode.models;

import java.time.LocalDate;

public class TaskDTO {
    private Integer taskID;
    private String taskName;
    private Boolean isClosed;
    private LocalDate closedDate;

    public Integer getTaskID() {
        return taskID;
    }

    public void setTaskID(Integer taskID) {
        this.taskID = taskID;
    }

    public String getTaskName() {
        return taskName;
    }

    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }

    public Boolean getClosed() {
        return isClosed;
    }

    public void setClosed(Boolean closed) {
        isClosed = closed;
    }

    public LocalDate getClosedDate() {
        return closedDate;
    }

    public void setClosedDate(LocalDate closedDate) {
        this.closedDate = closedDate;
    }

    public TaskDTO() {
    }

    public TaskDTO(Integer taskID, String taskName, Boolean isClosed, LocalDate closedDate) {
        this.taskID = taskID;
        this.taskName = taskName;
        this.isClosed = isClosed;
        this.closedDate = closedDate;
    }

    @Override
    public String toString() {
        return "TaskDTO{" +
                "taskID=" + taskID +
                ", taskName='" + taskName +
                ", isClosed=" + isClosed +
                ", closedDate=" + closedDate +
                '}';
    }
}