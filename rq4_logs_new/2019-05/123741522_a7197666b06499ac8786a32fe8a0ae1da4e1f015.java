package com.example.wilsonzhu.calendar_app.activity;

import android.Manifest;
import android.content.pm.PackageManager;
import android.os.Bundle;
import android.support.annotation.NonNull;
import android.support.v4.app.ActivityCompat;
import android.util.Log;
import android.widget.TextView;

import com.example.wilsonzhu.calendar_app.R;
import com.google.android.gms.awareness.Awareness;
import com.google.android.gms.awareness.snapshot.WeatherResponse;
import com.google.android.gms.awareness.state.Weather;
import com.google.android.gms.common.api.GoogleApiClient;
import com.google.android.gms.tasks.OnFailureListener;
import com.google.android.gms.tasks.OnSuccessListener;


public class WeatherActivity extends BaseActivity {
    private final String TAG = getClass().getCanonicalName();
    private final int MY_PERMISSIONS_REQUEST_FINE_LOCATION = 1;
    private TextView temp;
    private TextView feelsliketemp;
    private TextView condition;

    @Override
    protected void onCreate(Bundle onSavedInstance) {
        super.onCreate(onSavedInstance);
        GoogleApiClient client = new GoogleApiClient.Builder(this)
                .addApi(Awareness.getSnapshotClient(this).getApi())
                .build();
        client.connect();
        setContentView(R.layout.weather_page);
        setUp();
        checkAndRequestWeatherPermission();
    }

    private void setUp() {
        temp = findViewById(R.id.temperature);
        temp.setText("");
        feelsliketemp = findViewById(R.id.feels_like_temperature);
        condition = findViewById(R.id.condition);
    }

    private void getWeatherSnapshot() {
        if (ActivityCompat.checkSelfPermission(this, Manifest.permission.ACCESS_FINE_LOCATION) == PackageManager.PERMISSION_GRANTED) {
            Awareness.getSnapshotClient(this).getWeather().addOnSuccessListener(new OnSuccessListener<WeatherResponse>() {
                @Override
                public void onSuccess(WeatherResponse weatherResponse) {
                    Weather weather = weatherResponse.getWeather();
                    temp.setText(String.valueOf(weather.getTemperature(2)));
                    feelsliketemp.setText(String.valueOf(weather.getFeelsLikeTemperature(2)));
                    condition.setText(weather.getConditions()[0]);
                }
            }).addOnFailureListener(new OnFailureListener() {
                @Override
                public void onFailure(@NonNull Exception e) {
                    Log.e(TAG, "Failed to get weather " + e);
                }
            });

        }
    }

    private void checkAndRequestWeatherPermission() {
        if (ActivityCompat.checkSelfPermission(this, Manifest.permission.ACCESS_FINE_LOCATION) != PackageManager.PERMISSION_GRANTED) {
            if (ActivityCompat.shouldShowRequestPermissionRationale(this,
                    Manifest.permission.ACCESS_FINE_LOCATION)) {
                temp.setText("this app requires this to display weather info");
            } else {
                // No explanation needed; request the permission
                ActivityCompat.requestPermissions(this,
                        new String[]{Manifest.permission.ACCESS_FINE_LOCATION},
                        MY_PERMISSIONS_REQUEST_FINE_LOCATION);
            }
        } else {
            getWeatherSnapshot();
        }
    }

    @Override
    public void onRequestPermissionsResult(int requestCode, @NonNull String permissions[],
                                           @NonNull int[] grantResults) {
        switch (requestCode) {
            case MY_PERMISSIONS_REQUEST_FINE_LOCATION: {
                // If request is cancelled, the result arrays are empty.
                if (grantResults.length > 0
                        && grantResults[0] == PackageManager.PERMISSION_GRANTED) {
                    getWeatherSnapshot();
                } else {
                    Log.i(TAG, "Location permission denied.  Weather snapshot skipped.");
                }
            }
        }
    }
}