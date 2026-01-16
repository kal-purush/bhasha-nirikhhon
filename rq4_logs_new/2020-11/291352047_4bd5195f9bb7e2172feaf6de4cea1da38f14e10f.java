package com.operationcodify.cavoid.activities;

import androidx.annotation.NonNull;
import androidx.appcompat.app.AppCompatActivity;

import android.content.Intent;
import android.os.Bundle;
import android.view.Menu;
import android.view.MenuInflater;
import android.view.MenuItem;

import com.google.android.material.bottomnavigation.BottomNavigationView;
import com.operationcodify.cavoid.R;

public class GeneralInformationActivity extends AppCompatActivity {

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_general_information);

        getSupportActionBar().setTitle("General Information");

        BottomNavigationView bottomNavigationView = (BottomNavigationView) findViewById(R.id.bottom_navigation_menu);
        bottomNavigationView.setSelectedItemId(R.id.generalInfoBottomMenu);
        bottomNavigationView.setOnNavigationItemSelectedListener(new BottomNavigationView.OnNavigationItemSelectedListener() {
            @Override
            public boolean onNavigationItemSelected(@NonNull MenuItem item) {
                switch (item.getItemId()) {
                    case R.id.dashboardBottomMenu:
                        Intent dashboardIntent = new Intent(GeneralInformationActivity.this, DashboardActivity.class);
                        startActivity(dashboardIntent);
                        break;
                    case R.id.pastLocationBottomMenu:
                        Intent generalInfoIntent = new Intent(GeneralInformationActivity.this, PastLocationActivity.class);
                        startActivity(generalInfoIntent);
                        break;
                    case R.id.graphBottomMenu:
                        Intent mapIntent = new Intent(GeneralInformationActivity.this, GraphActivity.class);
                        startActivity(mapIntent);
                        break;
                }
                return true;
            }
        });

    }

    /**
     * displays the menu in the top toolbar
     */
    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        MenuInflater inflater = getMenuInflater();
        inflater.inflate(R.menu.main_menu, menu);
        return true;
    }

    /**
     * switches to the corresponding activity based on the activity selected by the user in the menu
     */
    public boolean onOptionsItemSelected(MenuItem item) {
        switch (item.getItemId()) {
            case R.id.action_settings:
                Intent settingsIntent = new Intent(GeneralInformationActivity.this, SettingsActivity.class);
                this.startActivity(settingsIntent);
                break;
            case R.id.action_covidDataInfo:
                Intent appInfoIntent = new Intent(GeneralInformationActivity.this, CovidDataActivity.class);
                this.startActivity(appInfoIntent);
                break;
        }
        return super.onOptionsItemSelected(item);
    }
}