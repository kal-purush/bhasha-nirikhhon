package com.payline.payment.oney.bean.common;

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;

public class OneyError40x extends OneyBean {

    @SerializedName("statusCode")
    private String statusCode;
    @SerializedName("message")
    private String message = "";
    @SerializedName("errorCode")
    private String errorCode;
    @SerializedName("errorMessage")
    private String errorMessage = "";

    public String getStatusCode() {
        return statusCode;
    }

    public String getMessage() {
        return message;
    }

    public String getErrorCode() {
        return errorCode;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public String getPrintableMessage() {
        return message + errorMessage;
    }

    public static OneyError40x parseJson(String json) {
        Gson gson = new Gson();
        return gson.fromJson(json, OneyError40x.class);

    }
}