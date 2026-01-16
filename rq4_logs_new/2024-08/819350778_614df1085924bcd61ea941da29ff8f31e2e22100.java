package com.app.Regional_News.data;

import com.google.gson.annotations.SerializedName;

import java.util.ArrayList;

public class Selected_pdf_data {
    @SerializedName("status")
    public String status;

    @SerializedName("msg")
    public String msg;

    @SerializedName("fetch_selected_pdf")
    public ArrayList<Selected_pdf_listdata> Selected_pdf_listdata;

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

    public ArrayList<com.app.Regional_News.data.Selected_pdf_listdata> getSelected_pdf_listdata() {
        return Selected_pdf_listdata;
    }

    public void setSelected_pdf_listdata(ArrayList<com.app.Regional_News.data.Selected_pdf_listdata> selected_pdf_listdata) {
        Selected_pdf_listdata = selected_pdf_listdata;
    }
}