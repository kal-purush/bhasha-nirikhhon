package pl.maxmati.tobiasz.mmos.bread.api;

import android.app.Activity;
import android.app.NotificationManager;
import android.content.Context;
import android.content.Intent;
import android.os.AsyncTask;
import android.util.Log;

import pl.maxmati.tobiasz.mmos.bread.R;
import pl.maxmati.tobiasz.mmos.bread.api.session.AuthenticationException;
import pl.maxmati.tobiasz.mmos.bread.api.session.InvalidPasswordException;
import pl.maxmati.tobiasz.mmos.bread.api.session.SessionException;
import pl.maxmati.tobiasz.mmos.bread.api.session.SessionManager;
import pl.maxmati.tobiasz.mmos.bread.api.session.UserNotFoundException;

/**
 * Created by mmos on 15.02.16.
 *
 * @author mmos
 *
 */

/**
 * Represents an asynchronous login/registration task used to authenticate
 * the user.
 */
public class LoginTask extends AsyncTask<Void, Void, Boolean> {
    private static final String TAG = "LoginTask";

    private final APIAuthActivity apiAuthActivity;
    private final String username;
    private final String password;

    SessionException sessionException;

    public LoginTask(APIAuthActivity apiAuthActivity, String username, String password) {
        this.apiAuthActivity = apiAuthActivity;
        this.username = username;
        this.password = password;
    }

    @Override
    protected Boolean doInBackground(Void... voids) {
        try {
            Session session = SessionManager.create(username, password);
            SessionManager.storeSession(apiAuthActivity.getApplicationContext(), session);
        } catch (SessionException e) {
            sessionException = e;
            return null;
        }
        return true;
    }

    @Override
    protected void onPostExecute(Boolean success) {
        onCancelled();

        if(success) {
            apiAuthActivity.setResult(Activity.RESULT_OK);
            apiAuthActivity.finish();

            if(apiAuthActivity.getIntent().hasExtra(APIAuthActivity.EXTRA_AUTH_NOTIFICATION_ID))
                removeAuthNotification(apiAuthActivity.getIntent().getIntExtra(APIAuthActivity
                        .EXTRA_AUTH_NOTIFICATION_ID, 0));
            if(apiAuthActivity.getIntent().hasExtra(APIAuthActivity.EXTRA_AUTH_SERVICE_INTENT))
                apiAuthActivity.startService(apiAuthActivity.getIntent().<Intent>getParcelableExtra(
                        APIAuthActivity.EXTRA_AUTH_SERVICE_INTENT));
        } else {
            if (sessionException instanceof AuthenticationException) {
                if (sessionException.getCause() instanceof UserNotFoundException) {
                    apiAuthActivity.mUsernameView.setError(apiAuthActivity.getString(R.string
                            .error_invalid_username));
                    apiAuthActivity.mUsernameView.requestFocus();
                } else if (sessionException.getCause() instanceof InvalidPasswordException) {
                    apiAuthActivity.mPasswordView.setError(apiAuthActivity.getString(R.string
                            .error_incorrect_password));
                    apiAuthActivity.mPasswordView.requestFocus();
                } else {
                    Log.e(TAG, "Session creation failed: " + sessionException
                            .getMessage(), sessionException);
                }
            }
        }
    }

    @Override
    protected void onCancelled() {
        apiAuthActivity.mAuthTask = null;
        apiAuthActivity.showProgress(false);
    }

    private void removeAuthNotification(int id) {
        NotificationManager notificationManager = (NotificationManager) apiAuthActivity.getSystemService(Context
                .NOTIFICATION_SERVICE);
        notificationManager.cancel(id);
    }
}