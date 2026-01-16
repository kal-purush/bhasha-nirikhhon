package com.wastedtimecounter.preferences.support;

import android.content.Context;
import android.preference.PreferenceManager;

import com.wastedtimecounter.R;

/**
 * Class-helper for theme preference.
 * <p/>
 * Created by Eugene on 1/24/2016.
 */
public class ThemeSupport {

    /**
     * Set application theme if it was changed in preferences.
     */
    static public void setActivityTheme(Context context) {
        switch (PreferenceManager.getDefaultSharedPreferences(context)
                .getInt("theme", R.string.pref_view_theme_light)) {

            case R.string.pref_view_theme_dark:
                context.setTheme(R.style.AppTheme_Dark);
                break;

            case R.string.pref_view_theme_light:
                context.setTheme(R.style.AppTheme);
                break;
        }
    }

}