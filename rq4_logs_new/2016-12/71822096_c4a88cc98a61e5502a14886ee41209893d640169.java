package com.example.hisaki.netflix;

import android.app.Application;

import com.example.hisaki.netflix.retrofit.NetflixApi;

import retrofit2.Retrofit;
import retrofit2.converter.gson.GsonConverterFactory;

/**
 * Created by hisaki on 09.12.2016.
 */

public class MyApp extends Application {


    private Retrofit retrofit;
    private static NetflixApi netflixApi;
    @Override
    public void onCreate() {
        super.onCreate();

        retrofit = new Retrofit.Builder()
                .baseUrl("http://netflixroulette.net/api/")
                .addConverterFactory(GsonConverterFactory.create())
                .build();

        netflixApi = retrofit.create(NetflixApi.class);
    }

    public static NetflixApi getNetflixAPI(){
        return netflixApi;
    }
}