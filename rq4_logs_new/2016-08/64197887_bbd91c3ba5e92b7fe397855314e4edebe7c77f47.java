package com.prabhu.codepath.nytimessearch.utils;

import android.content.Context;
import android.content.Intent;
import android.graphics.Color;
import android.net.ConnectivityManager;
import android.net.NetworkInfo;
import android.provider.Settings;
import android.support.design.widget.Snackbar;
import android.view.View;

import com.prabhu.codepath.nytimessearch.R;

import java.io.IOException;

/**
 * Created by pbabu on 7/31/16.
 */
public class Helper {

    public static boolean isNetworkAvailable(Context context) {
        ConnectivityManager connectivityManager =
                (ConnectivityManager)context.getSystemService(Context.CONNECTIVITY_SERVICE);
        final NetworkInfo networkInfo = connectivityManager.getActiveNetworkInfo();
        return networkInfo != null && networkInfo.isConnectedOrConnecting();
    }

    public static boolean isOnline() {
        Runtime runtime = Runtime.getRuntime();
        try {
            final java.lang.Process process = runtime.exec("/system/bin/ping -c 1 8.8.8.8");
            int exitValue = process.waitFor();
            return (exitValue == 0);
        }catch (IOException ex){
            ex.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return false;
    }

    public static void showSnackBar(View view, Context context) {
        Snackbar.make(view,
                R.string.no_internet_text,
                Snackbar.LENGTH_INDEFINITE)
                .setAction(R.string.snackbar_internet_action_text,
                        getSnackBarActionOnClickListener(context))
                .setActionTextColor(Color.YELLOW)
                .setDuration(5000)
                .show();
    }

    private static View.OnClickListener getSnackBarActionOnClickListener(final Context context) {
        return new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                Intent settingsIntent = new Intent(Settings.ACTION_SETTINGS);
                context.startActivity(settingsIntent);
            }
        };
    }
}