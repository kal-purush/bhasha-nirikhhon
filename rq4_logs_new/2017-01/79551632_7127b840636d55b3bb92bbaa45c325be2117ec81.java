package com.epam.traing.gitcl.helper;

import android.content.Context;
import android.content.SharedPreferences;
import android.preference.PreferenceManager;

import com.epam.traing.gitcl.R;

/**
 * Created by off on 22.01.2017.
 */

public class PrefHelper {
    private static final String FLAG_FIRST_START = "flag_first_start";

    public static SharedPreferences getSharedPreference(Context ctx) {
        return PreferenceManager.getDefaultSharedPreferences(ctx);
    }

    public static boolean showLoginScreen(Context ctx) {
        return isFirstStart(ctx);
    }

    public static boolean isFirstStart(Context ctx) {
        return getSharedPreference(ctx).getBoolean(FLAG_FIRST_START, true);
    }
}