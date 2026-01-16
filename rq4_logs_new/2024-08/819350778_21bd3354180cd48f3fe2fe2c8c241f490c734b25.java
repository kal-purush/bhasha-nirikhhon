package com.app.Regional_News.data;

import com.google.gson.annotations.SerializedName;

import java.util.ArrayList;

public class Event_cal_data {

    @SerializedName("status")
    public String status;

    @SerializedName("msg")
    public String msg;

    @SerializedName("event_cal_list")
    public ArrayList<Event_cal_listdata> Event_cal_listdata;

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public ArrayList<com.app.Regional_News.data.Event_cal_listdata> getEvent_cal_listdata() {
        return Event_cal_listdata;
    }

    public void setEvent_cal_listdata(ArrayList<com.app.Regional_News.data.Event_cal_listdata> event_cal_listdata) {
        Event_cal_listdata = event_cal_listdata;
    }
}