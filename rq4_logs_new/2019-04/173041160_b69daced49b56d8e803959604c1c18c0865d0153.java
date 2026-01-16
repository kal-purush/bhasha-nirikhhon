package np.com.naxa.factsnepal.common.livesharedpref;

import android.arch.lifecycle.LiveData;
import android.content.SharedPreferences;

public abstract class LiveSharePref<T> extends LiveData<T> {
    private String key;
    private T defaultValue;
    private SharedPreferences sharedPreferences;

    public LiveSharePref(SharedPreferences sharedPreferences, String key, T defaultValue) {
        this.sharedPreferences = sharedPreferences;
        this.key = key;
        this.defaultValue = defaultValue;
    }


    private SharedPreferences.OnSharedPreferenceChangeListener onPreferenceChangeListener = (sharedPreferences, key) -> {
        if (this.key.equals(key)) {
            setValue(getValueFromPreferences(key, defaultValue));
        }
    };

    abstract T getValueFromPreferences(String key, T defaultValue);

    @Override
    protected void onActive() {

        super.onActive();
        setValue(getValueFromPreferences(key, defaultValue));
        sharedPreferences.registerOnSharedPreferenceChangeListener(onPreferenceChangeListener);

    }

    @Override
    protected void onInactive() {
        sharedPreferences.unregisterOnSharedPreferenceChangeListener(onPreferenceChangeListener);
        super.onInactive();
    }
}
