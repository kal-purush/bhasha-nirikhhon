package com.hackharvard.smartmusicshuffle;

import android.app.Application;

import retrofit2.Retrofit;
import retrofit2.converter.gson.GsonConverterFactory;

public class AndroidApplication extends Application {

    public static final String API_URL = "https://api.projectoxford.ai/emotion/v1.0/recognize";

    public Retrofit retrofit;

    @Override
    public void onCreate() {
        initializeRetrofit();
    }

    public void initializeRetrofit() {
        retrofit = new Retrofit.Builder()
                .baseUrl(API_URL)
                .addConverterFactory(GsonConverterFactory.create())
                .build();
    }
}