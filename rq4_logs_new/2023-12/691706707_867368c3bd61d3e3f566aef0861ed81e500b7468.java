package com.immortalidiot.studentapp.auth;

import android.annotation.SuppressLint;import android.content.Context;
import android.content.SharedPreferences;

public class AuthManager {
    private static final String PREFERENCE_NAME = "auth";
    private static final String SECRET_KEY = "26fe1746b40acf3f263de2736060b6dceeafb8e0b140de23d9f59dbf11764e41";

    private static SharedPreferences getPreferences(Context context) {
        return context.getSharedPreferences(PREFERENCE_NAME, Context.MODE_PRIVATE);
    }

    @SuppressLint("CommitPrefEdits")
    public static void saveToken(Context context, String token) {
        getPreferences(context).edit().putString(SECRET_KEY, token);
    }

    public static String getToken(Context context) {
        return getPreferences(context).getString(SECRET_KEY, null);
    }

    public static void clearToken(Context context) {
        getPreferences(context).edit().remove(SECRET_KEY).apply();
    }

    public static boolean isLoggedIn(Context context) {
        return getToken(context) != null;
    }
}