
package services.cravefood.react.toast;

import java.util.*;

import android.widget.Toast;

import com.facebook.react.bridge.ReactApplicationContext;
import com.facebook.react.bridge.ReactContextBaseJavaModule;
import com.facebook.react.bridge.ReactMethod;


public class AndroidToast extends ReactContextBaseJavaModule {

    public AndroidToast (ReactApplicationContext reactContext) {
        super(reactContext);
    }

    @Override
    public String getName() {
        return "Torrada";
    }

    @Override
    public Map<String, Object> getConstants() {
        final Map<String, Object> constants = new HashMap<String, Object>();
        constants.put("SHORT", Toast.LENGTH_SHORT);
        constants.put("LONG", Toast.LENGTH_LONG);
        return constants;
    }

    @ReactMethod
    public void queimada(String message, int duration) {
        Toast.makeText(getReactApplicationContext(), message, duration).show();
    }
}