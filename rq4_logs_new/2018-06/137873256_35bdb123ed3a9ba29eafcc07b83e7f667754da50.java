package com.example.admin.testingv1;

public class Event {
    private String title;
    private String startTime;
    private String endTime;
    private String date;
    private String remarks;

    public Event (String title, String startTime, String endTime, String date, String remarks) {
        this.title = title;
        this.startTime = startTime;
        this.endTime = endTime;
        this.date = date;
        this.remarks = remarks;
    }

    public String getTitle (){
        return title;
    }

    public String getStartTime (){
        return startTime;
    }

    public String getEndTime () {
        return endTime;
    }

    public String getDate () {
        return date;
    }

    public String getRemarks () {
        return remarks;
    }

    public void editTitle (String title) {
        this.title = title;
    }

    public void editStartTime (String startTime) {
        this.startTime = startTime;
    }
    public void editEndTime (String endTime) {
        this.endTime = endTime;
    }
    public void editRemarks (String remarks) {
        this.remarks = remarks;
    }
}