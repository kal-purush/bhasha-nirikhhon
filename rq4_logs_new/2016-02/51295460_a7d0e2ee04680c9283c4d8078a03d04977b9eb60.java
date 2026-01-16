package pl.maxmati.tobiasz.mmos.bread.gcm;

import android.content.Intent;

import com.google.android.gms.iid.InstanceIDListenerService;

/**
 * Created by mmos on 17.02.16.
 *
 * @author mmos
 */
public class IDListenerService extends InstanceIDListenerService {
    private static final String TAG = "GCMIDListener";

    @Override
    public void onTokenRefresh() {
        // Fetch updated Instance ID token and notify our app's server of any changes (if applicable).
        Intent intent = new Intent(this, RegistrationIntentService.class);
        startService(intent);
    }
}