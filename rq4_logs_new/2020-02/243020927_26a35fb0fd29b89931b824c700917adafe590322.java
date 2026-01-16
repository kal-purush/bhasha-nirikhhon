package com.study.coolweather.service;

import android.app.AlarmManager;
import android.app.PendingIntent;
import android.app.Service;
import android.content.Intent;
import android.content.SharedPreferences;
import android.os.IBinder;
import android.os.SystemClock;
import android.preference.PreferenceManager;

import com.study.coolweather.WeatherActivity;
import com.study.coolweather.gson.HeWeather6;
import com.study.coolweather.util.HttpUtil;
import com.study.coolweather.util.Utility;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;

import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.Response;

public class AutoUpdateService extends Service {
    public AutoUpdateService() {
    }

    @Override
    public IBinder onBind(Intent intent) {
        return null;
    }

    @Override
    public int onStartCommand(Intent intent, int flags, int startId) {

        updateWeather();
        updateBingPic();
        AlarmManager manager = (AlarmManager) getSystemService(ALARM_SERVICE);
        int anHour = 8 * 60 * 60 * 1000; //这是8小时的毫秒数
        long triggerAtTime = SystemClock.elapsedRealtime() + anHour;
        Intent i = new Intent(this, AutoUpdateService.class);
        PendingIntent pi = PendingIntent.getService(this, 0, i, 0);
        manager.cancel(pi);
        manager.set(AlarmManager.ELAPSED_REALTIME_WAKEUP, triggerAtTime, pi);
        return super.onStartCommand(intent, flags, startId);
    }

    /**
     * 更新天气信息
     */
    private void updateWeather() {

        SharedPreferences prefs = PreferenceManager.getDefaultSharedPreferences(this);
        String weatherString = prefs.getString("weather", null);
        if (weatherString != null) {
            //有缓存时直接解析天气数据
            HeWeather6 weather = Utility.handleWeatherResponse(weatherString);
            String weatherId = weather.basic.location;
            //实况天气
            String nowUrl = "https://free-api.heweather.net/s6/weather/now?location="  +
                    weatherId + "&key=e65440bd890e4bd1bcd8af2e3e734413";
            HttpUtil.sendOkHttpRequest(nowUrl, new Callback() {
                @Override
                public void onFailure(@NotNull Call call, @NotNull IOException e) {
                    e.printStackTrace();
                }

                @Override
                public void onResponse(@NotNull Call call, @NotNull Response response) throws IOException {
//                    String responseText = response.body().string();
//                    HeWeather6 weather = Utility.handleWeatherResponse(responseText);
//                    if (weather != null && "ok".equals(weather.status)) {
//                            SharedPreferences.Editor editor = PreferenceManager.getDefaultSharedPreferences(WeatherActivity.this).edit();
//                            editor.putString("weather", responseText);
//                            editor.apply();
//                    }
                }
            });

            //生活指数
            String lifeStyleUrl = "https://free-api.heweather.net/s6/weather/lifestyle?location="  +
                    weatherId + "&key=e65440bd890e4bd1bcd8af2e3e734413";
            HttpUtil.sendOkHttpRequest(lifeStyleUrl, new Callback() {
                @Override
                public void onFailure(@NotNull Call call, @NotNull IOException e) {
                    e.printStackTrace();
                }

                @Override
                public void onResponse(@NotNull Call call, @NotNull Response response) throws IOException {
//                    String responseText = response.body().string();
//                    HeWeather6 weather = Utility.handleWeatherResponse(responseText);
//                            if (weather != null && "ok".equals(weather.status)) {
//                            SharedPreferences.Editor editor = PreferenceManager.getDefaultSharedPreferences(WeatherActivity.this).edit();
//                            editor.putString("weather", responseText);
//                            editor.apply();
//                            }
                }
            });

            //天气预报
            String forecastUrl = "https://free-api.heweather.net/s6/weather/forecast?location="  +
                    weatherId + "&key=e65440bd890e4bd1bcd8af2e3e734413";
            HttpUtil.sendOkHttpRequest(forecastUrl, new Callback() {
                @Override
                public void onFailure(@NotNull Call call, @NotNull IOException e) {
                    e.printStackTrace();
                }

                @Override
                public void onResponse(@NotNull Call call, @NotNull Response response) throws IOException {
//                    final String responseText = response.body().string();
//                    final HeWeather6 weather = Utility.handleWeatherResponse(responseText);
//                            if (weather != null && "ok".equals(weather.status)) {
//                            SharedPreferences.Editor editor = PreferenceManager.getDefaultSharedPreferences(WeatherActivity.this).edit();
//                            editor.putString("weather", responseText);
//                            editor.apply();
//                            }
                }
            });

            //空气质量
            String airUrl = "https://free-api.heweather.net/s6/air/now?location="  +
                    weatherId + "&key=e65440bd890e4bd1bcd8af2e3e734413";
            HttpUtil.sendOkHttpRequest(airUrl, new Callback() {
                @Override
                public void onFailure(@NotNull Call call, @NotNull IOException e) {
                    e.printStackTrace();
                }

                @Override
                public void onResponse(@NotNull Call call, @NotNull Response response) throws IOException {

//                    final String responseText = response.body().string();
//                    final HeWeather6 weather = Utility.handleWeatherResponse(responseText);
//                            if (weather != null && "ok".equals(weather.status)) {
//                            SharedPreferences.Editor editor = PreferenceManager.getDefaultSharedPreferences(WeatherActivity.this).edit();
//                            editor.putString("weather", responseText);
//                            editor.apply();
//                            }
                }
            });
        }
    }

    /**
     * 更新必应每日一图
     */
    private void updateBingPic() {
        String requestBingPic = "http://guolin.tech/api/bing_pic";
        HttpUtil.sendOkHttpRequest(requestBingPic, new Callback() {
            @Override
            public void onFailure(@NotNull Call call, @NotNull IOException e) {
                e.printStackTrace();
            }

            @Override
            public void onResponse(@NotNull Call call, @NotNull Response response) throws IOException {
                String bingPic = response.body().string();
                SharedPreferences.Editor editor = PreferenceManager.getDefaultSharedPreferences(WeatherActivity.this).edit();
                editor.putString("bing_pic", bingPic);
                editor.apply();
            }
        });
    }
}