package com.wastedtimecounter.preferences;

import android.content.Context;
import android.preference.Preference;
import android.util.AttributeSet;

import com.wastedtimecounter.R;

/**
 * Preference subclass for theme settings.
 * <p/>
 * Created by Eugene on 1/23/2016.
 */
public class ThemePreference extends Preference {

    /**
     * Constructor to create a Preference.
     *
     * @param context The Context in which to store Preference values.
     */
    public ThemePreference(Context context) {
        super(context);
    }


    /**
     * Constructor to create a Preference.
     *
     * @param context The Context in which to store Preference values.
     */
    public ThemePreference(Context context, AttributeSet attrs, int defStyleAttr) {
        super(context, attrs, defStyleAttr);
    }


    /**
     * Constructor to create a Preference.
     *
     * @param context The Context in which to store Preference values.
     */
    public ThemePreference(Context context, AttributeSet attrs) {
        super(context, attrs);
    }


    /**
     * Init value
     *
     * @param restorePersistedValue True to restore the persisted value;
     *                              false to use the given <var>defaultValue</var>.
     * @param defaultValue          The default value for this Preference. Only use this
     */
    @Override
    protected void onSetInitialValue(boolean restorePersistedValue, Object defaultValue) {
        if (restorePersistedValue) {
            // Restore existing state
            setTitle(getPersistedInt(R.string.pref_view_theme_light));
        } else {
            // Set default state from the XML attribute
            setTitle((Integer) defaultValue);
            persistInt(getTitleRes());
        }
    }


    /**
     * Save chosen theme when preference was clicked.
     */
    @Override
    protected void onClick() {
        switch (getTitleRes()) {
            case R.string.pref_view_theme_light:
                setTitle(R.string.pref_view_theme_dark);
                break;

            case R.string.pref_view_theme_dark:
                setTitle(R.string.pref_view_theme_light);
                break;

            default:
                setTitle(R.string.pref_view_theme_light);
        }

        persistInt(getTitleRes());
    }

}