package com.spimpalkar.pcubedemo.helpers;

import android.annotation.SuppressLint;
import android.content.BroadcastReceiver;
import android.content.Context;
import android.content.Intent;
import android.net.ConnectivityManager;
import android.net.Network;
import android.net.NetworkInfo;
import android.util.Log;

/**
 * Created by sheetal.pimpalkar on 3/07/2017.
 */

public class MPBroadcastReciever extends BroadcastReceiver
{
    private NetworkConnectionListener networkConnectionListener;
    private Context context;
    private boolean isConnected = false;

    public MPBroadcastReciever(NetworkConnectionListener networkConnectionListener){
        this.networkConnectionListener = networkConnectionListener;
    }

    @Override
    public void onReceive(Context context, Intent intent)
    {
        this.context = context;
        isNetworkAvailable(context);
        networkConnectionListener.onNetworkConnectionChanged(isConnected);

    }


    private boolean isNetworkAvailable(Context context)
    {

        ConnectivityManager connectivity = (ConnectivityManager) context.getSystemService(Context.CONNECTIVITY_SERVICE);
        if (connectivity != null)
        {
            NetworkInfo[] info = connectivity.getAllNetworkInfo();
            if (info != null) {
                for (int i = 0; i < info.length; i++) {
                    if (info[i].getState() == NetworkInfo.State.CONNECTED) {
                        if (!isConnected) {
                            Log.v("Now you are connected", " to Internet!");
                            //       Toast.makeText(context, "Internet available via Broadcast receiver", Toast.LENGTH_SHORT).show();
                            isConnected = true;
                            // do your processing here ---
                            // if you need to post any data to the server or get
                            // status
                            // update from the server

                        } else {
                            Log.v("You are not connected", " to Internet!");
                            //            Toast.makeText(context, "No Internet available via Broadcast receiver", Toast.LENGTH_SHORT).show();
                            isConnected = false;
                        }
                        return isConnected;

                    }

                }
            }

        }
                        return isConnected;
    }


    public void setOnNetworkChangeListener(NetworkConnectionListener networkChangeReceiverListener)
    {
        this.networkConnectionListener = networkChangeReceiverListener;
    }

}