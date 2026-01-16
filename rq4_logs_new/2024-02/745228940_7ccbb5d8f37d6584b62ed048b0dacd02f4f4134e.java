package org.example.JavaTaigaCode.models;

public class AdoptedWorkDTO {
    Integer adoptedWork;
    Integer sprintTotalPoints;
    Integer milestoneID;
    String milestoneName;

    public Integer getAdoptedWork() {
        return adoptedWork;
    }

    public void setAdoptedWork(Integer adoptedWork) {
        this.adoptedWork = adoptedWork;
    }

    public Integer getSprintTotalPoints() {
        return sprintTotalPoints;
    }

    public void setSprintTotalPoints(Integer sprintTotalPoints) {
        this.sprintTotalPoints = sprintTotalPoints;
    }

    public Integer getMilestoneID() {
        return milestoneID;
    }

    public void setMilestoneID(Integer milestoneID) {
        this.milestoneID = milestoneID;
    }
    
    public String getMilestoneName() {
        return milestoneName;
    }

    public void setMilestoneName(String milestoneName) {
        this.milestoneName = milestoneName;
    }
    
    public AdoptedWorkDTO(Integer adoptedWork, Integer sprintTotalPoints, Integer milestoneID) {
        this.adoptedWork = adoptedWork;
        this.sprintTotalPoints = sprintTotalPoints;
        this.milestoneID = milestoneID;
    }
}