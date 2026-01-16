package com.app.Regional_News.data;

import com.google.gson.annotations.SerializedName;

public class RN_Udata {
    @SerializedName("msg")
    public String msg;
    @SerializedName("status")
    public String status;
    @SerializedName("u_id")
    public String u_id;
    @SerializedName("u_name")
    public String u_name;
    @SerializedName("u_email")
    public String u_email;
    @SerializedName("u_pwd")
    public String u_pwd;
    @SerializedName("u_pic")
    public String u_pic;
    @SerializedName("u_c_date")
    public String u_c_date;
    @SerializedName("u_status")
    public String u_status;

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

    public String getU_id() {
        return u_id;
    }

    public void setU_id(String u_id) {
        this.u_id = u_id;
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

    public String getU_pic() {
        return u_pic;
    }

    public void setU_pic(String u_pic) {
        this.u_pic = u_pic;
    }

    public String getU_c_date() {
        return u_c_date;
    }

    public void setU_c_date(String u_c_date) {
        this.u_c_date = u_c_date;
    }

    public String getU_status() {
        return u_status;
    }

    public void setU_status(String u_status) {
        this.u_status = u_status;
    }
}