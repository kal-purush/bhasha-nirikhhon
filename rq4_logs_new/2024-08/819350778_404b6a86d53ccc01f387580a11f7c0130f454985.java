package com.app.Regional_News.data;

import com.google.gson.annotations.SerializedName;

import java.util.ArrayList;

public class User_id_fetch_data {

    @SerializedName("status")
    private String status;

    @SerializedName("msg")
    private String msg;

    @SerializedName("fetchUid")
    private ArrayList<User_id_fetch_listdata> User_id_fetch_listdata;  // Change this type

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

    public ArrayList<com.app.Regional_News.data.User_id_fetch_listdata> getUser_id_fetch_listdata() {
        return User_id_fetch_listdata;
    }

    public void setUser_id_fetch_listdata(ArrayList<com.app.Regional_News.data.User_id_fetch_listdata> user_id_fetch_listdata) {
        User_id_fetch_listdata = user_id_fetch_listdata;
    }
}