package pl.maxmati.tobiasz.mmos.bread.widget;

import android.appwidget.AppWidgetManager;
import android.content.Intent;
import android.os.AsyncTask;
import android.os.Bundle;
import android.util.Log;
import android.widget.RemoteViews;

import pl.maxmati.tobiasz.mmos.bread.R;
import pl.maxmati.tobiasz.mmos.bread.api.APIConnector;
import pl.maxmati.tobiasz.mmos.bread.api.ApiLoginActivity;
import pl.maxmati.tobiasz.mmos.bread.api.BreadManager;
import pl.maxmati.tobiasz.mmos.bread.api.session.AuthenticationException;
import pl.maxmati.tobiasz.mmos.bread.api.session.InvalidPasswordException;
import pl.maxmati.tobiasz.mmos.bread.api.session.UserNotFoundException;

/**
 * Created by mmos on 11.02.16.
 *
 * @author mmos
 */
public class BreadWidgetConfigure extends ApiLoginActivity {
    private static final String TAG = "BreadWidgetConfigure";
    private static final String API_URI = "http://api.flat.maxmati.pl:8888/";

    private int mAppWidgetId = AppWidgetManager.INVALID_APPWIDGET_ID;

    @Override
    protected AsyncTask<Void, Void, Boolean> getLoginTask(final String username, final String password) {
        /**
         * Represents an asynchronous login/registration task used to authenticate
         * the user.
         */
        return new AsyncTask<Void, Void, Boolean>() {
            AuthenticationException loginException;

            @Override
            protected Boolean doInBackground(Void... params) {
                try {
                    APIConnector apiConnector = new APIConnector(API_URI, username, password);
                    Log.d(TAG, "Got " + BreadManager.get(apiConnector) + " breads");
                } catch (AuthenticationException e) {
                    loginException = e;
                    return null;
                }
                return true;
            }

            @Override
            protected void onPostExecute(final Boolean result) {
                mAuthTask = null;
                showProgress(false);

                if (result != null) {
                    startService(new Intent(BreadWidgetConfigure.this, BreadWidgetUpdater.class));

                    Intent resultValue = new Intent();
                    resultValue.putExtra(AppWidgetManager.EXTRA_APPWIDGET_ID, mAppWidgetId);

                    setResult(RESULT_OK, resultValue);
                    finish();
                } else {
                    if(loginException instanceof UserNotFoundException) {
                        mUsernameView.setError(getString(R.string.error_invalid_username));
                        mUsernameView.requestFocus();
                    } else if(loginException instanceof InvalidPasswordException) {
                        mPasswordView.setError(getString(R.string.error_incorrect_password));
                        mPasswordView.requestFocus();
                    } else {
                        Log.e(TAG, "Session creation failed: " + loginException.getMessage(), loginException);
                    }
                }
            }

            @Override
            protected void onCancelled() {
                mAuthTask = null;
                showProgress(false);
            }
        };
    }

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        Bundle extras;

        setResult(RESULT_CANCELED);

        extras = getIntent().getExtras();
        if (extras != null) {
            mAppWidgetId = extras.getInt(
                    AppWidgetManager.EXTRA_APPWIDGET_ID,
                    AppWidgetManager.INVALID_APPWIDGET_ID);
        }
        if (mAppWidgetId == AppWidgetManager.INVALID_APPWIDGET_ID)
            finish();

        super.onCreate(savedInstanceState);
    }
}