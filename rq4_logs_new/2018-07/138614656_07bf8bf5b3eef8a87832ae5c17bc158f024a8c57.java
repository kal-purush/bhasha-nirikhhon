package com.andela.javadevelopers.util;

import android.content.Context;
import android.net.ConnectivityManager;
import android.net.NetworkInfo;

/**
 * Created by andeladeveloper on 13/07/2018.
 */
public final class Connectivity {
    /**
     * Connectivity.
     */
    private Connectivity() {
        // left blank intentionally
    }

    /**
     * Is connected boolean.
     *
     * @param context the context
     * @return the boolean
     */
    public static boolean isConnected(Context context) {
        ConnectivityManager cm = (ConnectivityManager)
                context.getSystemService(Context.CONNECTIVITY_SERVICE);
        assert cm != null;
        NetworkInfo info = cm.getActiveNetworkInfo();
        return info != null && info.isConnected();
    }
}