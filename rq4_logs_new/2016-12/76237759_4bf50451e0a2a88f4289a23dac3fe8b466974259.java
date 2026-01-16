package com.example.finalproject.activities;

import android.content.Intent;
import android.support.annotation.NonNull;
import android.support.design.widget.NavigationView;
import android.support.v4.widget.DrawerLayout;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.view.MenuItem;

import com.example.finalproject.R;

public class HistoryActivity extends AppCompatActivity {
    private DrawerLayout drawerLayout;
    private NavigationView navigationView;


    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_history);

        findView();
        setupDrawerContent(navigationView);
    }

    private void findView() {
        drawerLayout = (DrawerLayout) findViewById(R.id.drawerLayout);
        navigationView = (NavigationView) findViewById(R.id.nvMenu);
    }

    private void setupDrawerContent(NavigationView navigationView) {
        navigationView.setNavigationItemSelectedListener(new NavigationView.OnNavigationItemSelectedListener() {
            @Override
            public boolean onNavigationItemSelected(@NonNull MenuItem item) {
                item.setChecked(true);
                Intent intent;
                drawerLayout.closeDrawers();
                switch (item.getItemId()) {
                    case R.id.home:
                        intent = new Intent(HistoryActivity.this, MainActivity.class);
                        startActivity(intent);
                        break;
                    case R.id.rankingList:
                        intent = new Intent(HistoryActivity.this, RankingListActivity.class);
                        startActivity(intent);
                        break;
                    case R.id.steps:
                        intent = new Intent(HistoryActivity.this, CounterActivity.class);
                        startActivity(intent);
                        break;
                    case R.id.settings:
                        intent = new Intent(HistoryActivity.this, SettingsActivity.class);
                        startActivity(intent);
                        break;
                    case R.id.history:
                        item.setChecked(false);
                        break;
                    default:
                        break;
                }
                return true;
            }
        });
    }
}