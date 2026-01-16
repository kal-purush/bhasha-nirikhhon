package com.foo.umbrella.service;

import android.app.Application;
import android.app.IntentService;
import android.content.Intent;
import android.os.Bundle;
import android.support.annotation.Nullable;
import android.util.Log;

import com.foo.umbrella.data.ApiServicesProvider;
import com.foo.umbrella.data.model.WeatherData;

import java.io.IOException;

import retrofit2.Response;

/**
 * Created by Hector Vera on 9/7/2017.
 */

public class CommunicationService extends IntentService {

    private static final String DEBUG = "DEBUG_INTENT_SERVICE";
    public CommunicationService(){
        super("CommunicationService");
    }
    @Override
    protected void onHandleIntent(@Nullable Intent intent) {
        Log.d(DEBUG, "onHandleIntent");
        String zipCode = intent.getStringExtra("zipCode");
        ApiServicesProvider api = new ApiServicesProvider(getApplication());
        try {
            Log.d(DEBUG, "try");
            Response<WeatherData> response = api.getWeatherService().forecastForZipCallable(zipCode).execute();
            WeatherData weatherData = response.body();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}