package com.example.enigmator.utils;


import android.content.Context;
import com.example.enigmator.controller.HttpManager;
import com.example.enigmator.controller.HttpRequest;


public final class HttpRequestGenerator {
    private HttpManager httpManager;

    public HttpRequestGenerator(Context context) {
        httpManager = new HttpManager(context);
    }

    public void requestMediaOfEnigma(long enigmaId, HttpRequest.HttpRequestListener listener) {
        httpManager.addToQueue(HttpRequest.GET,
                "/api/Media?filter[where][enigmeID]=" + enigmaId,
                null,
                listener);
    }

    public void downloadMediaOfEnigma(String fileName, HttpRequest.HttpRequestListener listener) {
        httpManager.addToQueue(HttpRequest.GET,
                "/api/Containers/enigme/download/" + fileName,
                null,
                listener);
    }
}