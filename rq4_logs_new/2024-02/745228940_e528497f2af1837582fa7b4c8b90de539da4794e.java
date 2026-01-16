package org.example.JavaTaigaCode.models;

import java.time.LocalDate;

public class UserStoryDTO {
    private Integer userStoryID;
    private Integer businessValue;
    private boolean isClosed;
    private LocalDate finishDate;

    public Integer getUserStoryID() {
        return userStoryID;
    }

    public void setUserStoryID(Integer userStoryID) {
        this.userStoryID = userStoryID;
    }

    public Integer getBusinessValue() {
        return businessValue;
    }

    public void setBusinessValue(Integer businessValue) {
        this.businessValue = businessValue;
    }

    public boolean isClosed() {
        return isClosed;
    }

    public void setClosed(boolean closed) {
        isClosed = closed;
    }

    public LocalDate getFinishDate() {
        return finishDate;
    }

    public void setFinishDate(LocalDate finishDate) {
        this.finishDate = finishDate;
    }

    public UserStoryDTO(Integer userStoryID, Integer businessValue, boolean isClosed, LocalDate finishDate) {
        this.userStoryID = userStoryID;
        this.businessValue = businessValue;
        this.isClosed = isClosed;
        this.finishDate = finishDate;
    }
}