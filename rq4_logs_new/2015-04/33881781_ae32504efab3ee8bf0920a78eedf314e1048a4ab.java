package com.example.nermingraca.bitbayapp;

import android.content.Intent;
import android.support.v7.app.ActionBarActivity;
import android.os.Bundle;
import android.view.Menu;
import android.view.MenuItem;
import android.widget.TextView;

import com.example.nermingraca.bitbayapp.models.User;
import com.example.nermingraca.bitbayapp.singletons.UserData;
import com.example.nermingraca.bitbayapp.singletons.UserFeed;


public class ProfileActivity extends ActionBarActivity {

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_profile);

        Intent intent = getIntent();
        String username = intent.getStringExtra("username");
        String email = intent.getStringExtra("email");

        TextView usernameV = (TextView) findViewById(R.id.tvUsername);
        TextView emailV = (TextView) findViewById(R.id.tvEmail);

        usernameV.setText(username);
        emailV.setText(email);
    }

    public static User getCurrentUser() {
        int id = UserData.getInstance().getId();
        String url = String.format("http://10.0.2.2:9000/api/showuser/%d", id);

        UserFeed userFeed = UserFeed.getInstance();
        return userFeed.getFeed(url);
    }


    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.menu_profile, menu);
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

        if (id == R.id.logout_action) {
            MainActivity.logout();
            moveTaskToBack(true);
            Intent toLogin = new Intent( ProfileActivity.this, MainActivity.class);
            startActivity(toLogin);
            return true;
        }

        if (id == R.id.profile_action) {
            User user = ProfileActivity.getCurrentUser();
            Intent intent = new Intent(ProfileActivity.this, ProfileActivity.class);
            intent.putExtra("username", user.getmUsername());
            intent.putExtra("email", user.getmEmail());
            startActivity(intent);
            return true;
        }

        return super.onOptionsItemSelected(item);
    }
}