package hm.orz.chaos114.android.circlewatcher;

import android.content.Context;
import android.content.SharedPreferences;
import android.preference.PreferenceManager;

/**
 * Manage SharedPreferences.
 */
public final class SharedPreferenceUtil {
    private static final String KEY_API_TOKEN = "API_TOKEN";
    private SharedPreferenceUtil() {
        // prevent instantiate
    }

    public static void saveApiToken(Context context, String apiToken) {
        SharedPreferences sp = PreferenceManager.getDefaultSharedPreferences(context);
        sp.edit().putString(KEY_API_TOKEN, apiToken).apply();
    }

    public static String getApiToken(Context context) {
        SharedPreferences sp = PreferenceManager.getDefaultSharedPreferences(context);
        return sp.getString(KEY_API_TOKEN, null);
    }
}