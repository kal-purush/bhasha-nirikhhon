package com.app.Regional_News.data;

import com.google.gson.annotations.SerializedName;

public class Profile_Updationdata {
    @SerializedName("msg")
    public String msg;
    @SerializedName("status")
    public String status;
    @SerializedName("sm_u_id")
    public String sm_u_id;
    @SerializedName("u_name")
    public String u_name;
    @SerializedName("u_email")
    public String u_email;
    @SerializedName("u_pwd")
    public String u_pwd;

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getSm_u_id() {
        return sm_u_id;
    }

    public void setSm_u_id(String sm_u_id) {
        this.sm_u_id = sm_u_id;
    }

    public String getU_name() {
        return u_name;
    }

    public void setU_name(String u_name) {
        this.u_name = u_name;
    }

    public String getU_email() {
        return u_email;
    }

    public void setU_email(String u_email) {
        this.u_email = u_email;
    }

    public String getU_pwd() {
        return u_pwd;
    }

    public void setU_pwd(String u_pwd) {
        this.u_pwd = u_pwd;
    }
}