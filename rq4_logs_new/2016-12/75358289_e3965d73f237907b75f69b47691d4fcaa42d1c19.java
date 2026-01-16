package com.clubjevin.festivus;

import android.net.ConnectivityManager;
import android.net.NetworkInfo;
import android.view.View;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.Executors;
import java.util.concurrent.Phaser;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by kevin on 12/9/16.
 */

public class NetworkMonitor {
    private static NetworkMonitor instance;
    public static NetworkMonitor getInstance() {
        if(instance == null) {
            instance = new NetworkMonitor();
        }
        return instance;
    }

    private long NETWORK_CHECK_INTERVAL_MILLIS = 30 * 1000L;

    private AtomicBoolean networkOk = new AtomicBoolean(false);

    public NetworkMonitor() {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

        scheduler.scheduleWithFixedDelay(
                new Runnable() {
                    public void run() {
                        networkOk.set(isNetworkAvailable());
                    }
                }, 0, NETWORK_CHECK_INTERVAL_MILLIS, TimeUnit.MILLISECONDS
        );
    }

    public boolean isNetworkOk() {
        return networkOk.get();
    }

    private boolean isNetworkAvailable() {
        URL url = null;
        try {
            url = new URL("http://34.194.97.23:8080");
        } catch (MalformedURLException e) {
            e.printStackTrace();
            return false;
        }

        int responseCode = -1;
        try {
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setReadTimeout(2500);
            conn.setConnectTimeout(2500);
            conn.setRequestMethod("GET");
            conn.setDoInput(false);
            conn.setDoOutput(false);

            responseCode = conn.getResponseCode();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return responseCode / 100 == 2;
    }
}