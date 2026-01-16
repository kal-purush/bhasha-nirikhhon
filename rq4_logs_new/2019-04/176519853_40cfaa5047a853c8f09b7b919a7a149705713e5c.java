package com.app.petify.activities;

import android.content.Intent;
import android.os.Bundle;
import android.support.v7.app.AppCompatActivity;
import android.view.View;
import android.widget.Button;

import com.app.petify.R;
import com.app.petify.models.Driver;
import com.app.petify.utils.LocalStorage;
import com.facebook.login.LoginManager;

public class DriverHomeActivity extends AppCompatActivity {

    private Button logoutButton;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_home_driver);

        Driver driver = LocalStorage.getDriver();

        logoutButton = findViewById(R.id.logout_button);
        logoutButton.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                LoginManager.getInstance().logOut();
                Intent navigationIntent = new Intent(DriverHomeActivity.this, MainActivity.class);
                startActivity(navigationIntent);
            }
        });
    }
}