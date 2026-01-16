package com.achulkov.loftmon.remote;

import com.google.gson.annotations.SerializedName;

import com.google.gson.annotations.SerializedName;

public class StatusResp {

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getItemId() {
        return itemId;
    }

    public void setItemId(String itemId) {
        this.itemId = itemId;
    }

    @SerializedName("status")
    private String status;


    @SerializedName("id")
    private String itemId;

}