package co.johnnyli.gamer;

import android.content.Intent;
import android.graphics.Color;
import android.graphics.drawable.ColorDrawable;
import android.os.Bundle;
import android.support.v7.app.ActionBar;
import android.support.v7.app.ActionBarActivity;
import android.util.Base64;
import android.util.Log;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.Toast;

import com.loopj.android.http.AsyncHttpClient;
import com.loopj.android.http.JsonHttpResponseHandler;
import com.loopj.android.http.RequestParams;

import org.json.JSONObject;

public class Login extends ActionBarActivity implements View.OnClickListener{

    private String loginURL = "http://ec2-52-11-124-82.us-west-2.compute.amazonaws.com/rest-auth/login/";
    private String registerURL = "http://ec2-52-11-124-82.us-west-2.compute.amazonaws.com/rest-auth/registration/";
    private static String username;
    private static String password;
    public static String auth;
    private EditText user;
    private EditText pass;

    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_login);
        //ActionBar Color
        String color = MainActivity.color;
        ActionBar bar = getSupportActionBar();
        bar.setBackgroundDrawable(new ColorDrawable(Color.parseColor(color)));
        user = (EditText) findViewById(R.id.username);
        pass = (EditText) findViewById(R.id.password);
        Button login = (Button) findViewById(R.id.login);
        login.setOnClickListener(this);
        Button register = (Button) findViewById(R.id.register);
        register.setOnClickListener(this);
    }

    @Override
    public void onClick(View v) {
        final Intent feed = new Intent(this, Feed.class);
        if(v.getId() == R.id.login) {
            RequestParams params = new RequestParams();
            params.put("username", user.getText().toString());
            params.put("password", pass.getText().toString());
            AsyncHttpClient client = new AsyncHttpClient();
            client.addHeader("X-CSRFToken", Info.csrftoken);
            client.post(loginURL, params, new JsonHttpResponseHandler() {
                @Override
                public void onSuccess(JSONObject jsonObject) {
                    username = user.getText().toString();
                    password = pass.getText().toString();
                    auth = "Basic " +
                            Base64.encodeToString((username + ":" + password).getBytes(), Base64.NO_WRAP);
                    finish();
                    startActivity(feed);
                }
                @Override
                public void onFailure(int statusCode, Throwable throwable, JSONObject error) {
                    Toast.makeText(getApplicationContext(), "Error: Invalid username or password.",
                            Toast.LENGTH_LONG).show();
                    Log.d("Login fail", error.toString());
                }
            });
        } else if (v.getId() == R.id.register) {
            RequestParams params = new RequestParams();
            params.put("username", user.getText().toString());
            params.put("password1", pass.getText().toString());
            params.put("password2", pass.getText().toString());
            AsyncHttpClient client = new AsyncHttpClient();
            client.addHeader("X-CSRFToken", Info.csrftoken);
            client.post(registerURL, params, new JsonHttpResponseHandler() {
                @Override
                public void onSuccess(JSONObject jsonObject) {
                    username = user.getText().toString();
                    password = pass.getText().toString();
                    auth = "Basic " +
                            Base64.encodeToString((username + ":" + password).getBytes(), Base64.NO_WRAP);
                    finish();
                    startActivity(feed);
                }
                @Override
                public void onFailure(int statusCode, Throwable throwable, JSONObject error) {
                    Toast.makeText(getApplicationContext(), "Error: Invalid username or password.",
                            Toast.LENGTH_LONG).show();
                    Log.d("register fail", error.toString());
                }
            });
        }
    }
}