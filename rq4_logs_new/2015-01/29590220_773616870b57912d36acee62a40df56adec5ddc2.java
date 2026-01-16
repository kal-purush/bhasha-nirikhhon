package com.metafour.cwbay;

import android.support.v7.app.ActionBarActivity;
import android.os.Bundle;
import android.util.Log;
import android.view.Menu;
import android.view.MenuItem;
import android.view.View;
import android.widget.EditText;

import com.metafour.cwbay.model.User;
import com.metafour.cwbay.remote.WebAPI;
import com.metafour.cwbay.util.Constants;


public class WebAPIWorkbenchActivity extends ActionBarActivity {

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_web_apiworkbench);
    }


    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.menu_web_apiworkbench, menu);
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

    public void onClick(View v) {
        EditText in = (EditText)findViewById(R.id.editText);

        User u = new User();
        u.setEmail(in.getText().toString());

        WebAPI.userEdit(this, new WebAPI.Callback<User>() {
            @Override
            public void onFailed(String reason) {
                Log.i(Constants.ACTIVITY_LOG_TAG, reason);
            }

            @Override
            public void onSuccess(User responseObject) {
                Log.i(Constants.ACTIVITY_LOG_TAG, responseObject.toString());
            }
        }, "nadim@meta4.com", u);
    }
}