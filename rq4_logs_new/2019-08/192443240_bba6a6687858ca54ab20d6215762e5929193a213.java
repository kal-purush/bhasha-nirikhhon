package com.mobile.aquafish;

import android.content.Context;
import android.content.SharedPreferences;

import static android.content.Context.MODE_PRIVATE;

public class SharedPrefMain {

    private static final String AQUAFISH_APP = "aquafishApp";
    public static final String AQUA_CODE = "aquaCode";
    public static final String AQUA_NAME = "nameUser";

    private SharedPreferences preferences;
    private SharedPreferences.Editor editor;

    public SharedPrefMain(Context context) {
        preferences = context.getSharedPreferences(AQUAFISH_APP, MODE_PRIVATE);
        editor = preferences.edit();
        editor.apply();
    }

    public void saveStringCode(String key, String value) {
        editor.putString(key, value);
        editor.commit();
    }

    public void saveStringName(String key, String value) {
        editor.putString(key, value);
        editor.commit();
    }

    public String getAquaCode() {
        return preferences.getString(AQUA_CODE, "");
    }

    public String getAquaName() {
        return preferences.getString(AQUA_NAME, "");
    }
}