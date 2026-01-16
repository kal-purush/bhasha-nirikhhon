package ca.justinrichard.link;

import android.content.Intent;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.util.Log;

import com.amazonaws.mobile.user.IdentityManager;
import com.amazonaws.mobile.user.IdentityProvider;
import com.amazonaws.mobile.user.signin.SignInManager;
import com.amazonaws.mobile.user.signin.SignInProvider;

public class SplashActivity extends AppCompatActivity {

    private final String TAG = "SplashActivity";
    private SignInManager signInManager;

    /**
     * SignInResultsHandler handles the results from sign-in for a previously signed in user.
     */
    private class SignInResultsHandler implements IdentityManager.SignInResultsHandler {
        /**
         * Receives the successful sign-in result for an alraedy signed in user and starts the main
         * activity.
         * @param provider the identity provider used for sign-in.
         */
        @Override
        public void onSuccess(final IdentityProvider provider) {
            // The user is now signed in with the previously signed-in provider.
            Log.i(TAG, "User successfully refreshed credentials, passing on to MainActivity");

            // The sign-in manager is no longer needed once signed in.
            SignInManager.dispose();

            // Go to main activity
            Intent intent = new Intent(SplashActivity.this, MainActivity.class);
            startActivity(intent);
            finish();
        }

        /**
         * For the case where the user previously was signed in and an attempt is made to sign the
         * user back in again, there is not an option for the user to cancel, so this is overriden
         * as a stub.
         * @param provider the identity provider with which the user attempted sign-in.
         */
        @Override
        public void onCancel(final IdentityProvider provider) {
            Log.wtf(TAG, "Cancel can't happen when handling a previously sign-in user.");
        }

        /**
         * Receives the sign-in result that an error occurred signing in with the previously signed
         * in provider and re-directs the user to the sign-in activity to sign in again.
         * @param provider the identity provider with which the user attempted sign-in.
         * @param ex the exception that occurred.
         */
        @Override
        public void onError(final IdentityProvider provider, Exception ex) {
            // The user is not sign-ed in.
            Log.i(TAG, "Error on credential refresh, sending to sign in page");
            Intent intent = new Intent(SplashActivity.this, SignInActivity.class);
            startActivity(intent);
            finish();
        }
    }

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        Log.i(TAG, "Creating runnable thread to check login status");
        final Thread thread = new Thread(new Runnable() {
            public void run() {
                signInManager = SignInManager.getInstance(SplashActivity.this);
                final SignInProvider provider = signInManager.getPreviouslySignedInProvider();
                // If the user was already previously signed-in by a provider.
                if (provider != null) {
                    // asynchronously handle refreshing credentials and call our handler.
                    Log.i(TAG, "User was previously signed in, refreshing credentials and passing result to new sign in results handler");
                    signInManager.refreshCredentialsWithProvider(SplashActivity.this, provider, new SignInResultsHandler());
                } else {
                    // User was not previously signed in.
                    Log.i(TAG, "User not previously signed in, sending to SignInactivity");
                    Intent intent = new Intent(SplashActivity.this, SignInActivity.class);
                    startActivity(intent);
                    finish();
                }
            }
        });
        thread.start();
    }
}