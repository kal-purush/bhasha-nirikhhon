package com.metafour.cwbay;

import android.content.Context;
import android.os.Bundle;
import android.support.v7.app.ActionBarActivity;
import android.util.Log;
import android.view.Menu;
import android.view.MenuItem;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.ProgressBar;

import com.metafour.cwbay.model.User;
import com.metafour.cwbay.remote.WebAPI;
import com.metafour.cwbay.remote.WebConnection;
import com.metafour.cwbay.util.AppUtility;

/**
 * Created by Noor on 1/22/2015.
 */
public class SignInActivity extends ActionBarActivity implements WebConnection.Callback {
    private Context context;
    private Button loginButton;
    private EditText userEmail, password;
    private ProgressBar progressBar;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.login);

        this.context = getApplicationContext();
        this.loginButton = (Button)findViewById(R.id.signInBtn);
        this.userEmail = (EditText)findViewById(R.id.signInEmail);
        this.password = (EditText)findViewById(R.id.signInPassword);
        this.progressBar = (ProgressBar) findViewById(R.id.signInProgressBar);

        loginButton.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                doSignIn(v);
            }
        });
    }


    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.menu_main, menu);
        return true;
    }

    @Override
    public boolean onOptionsItemSelected(MenuItem item) {
        // Handle action bar item clicks here. The action bar will
        // automatically handle clicks on the Home/Up button, so long
        // as you specify a parent activity in AndroidManifest.xml.
        int id = item.getItemId();

        //noinspection SimplifiableIfStatement
        if (id == R.id.action_settings) {

            return true;
        }

        return super.onOptionsItemSelected(item);
    }

    @Override
    public void onResponse(WebConnection.Response response) {
        Log.i("CWBay", response.getStatus() + " " + response.getContent());
    }

    /**
     * Does sign in operation
     *
     * @param view @View reference
     */
    private void doSignIn(View view) {
        Log.i("CWBay", "Starts signing.......");
        if (userEmail.getText().toString().isEmpty() || password.getText().toString().isEmpty()) {
            AppUtility.showShortLengthToast(context, context.getResources().getString(R.string.login_cred_empty));
            return;
        }
        AppUtility.showProgressBarAndDisableButton(progressBar, loginButton);
        WebConnection.getInstance().connect(this, "http://192.168.0.57:8000");
        WebAPI.userLogin(this, new WebAPI.UserLoginCallback() {
            @Override
            public void onUserLoginFailed(String reason) {
                Log.i(AppUtility.LOG_TAG, "Login failed with reason : " + reason);
                AppUtility.showShortLengthToast(context, reason);
                AppUtility.hideProgressBarAndEnableButton(progressBar, loginButton);
            }

            @Override
            public void onUserLoginSuccess(User user) {
                Log.i(AppUtility.LOG_TAG, "Login successful. Login details is " + user.toString());
                AppUtility.hideProgressBarAndEnableButton(progressBar, loginButton);
            }
        }, userEmail.getText().toString(), password.getText().toString());
    }
}