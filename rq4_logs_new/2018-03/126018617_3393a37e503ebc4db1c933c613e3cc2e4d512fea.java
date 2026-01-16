package com.icemobile.icegreen;

import android.content.Intent;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.support.v7.widget.LinearLayoutManager;
import android.support.v7.widget.RecyclerView;
import android.view.View;

import java.util.ArrayList;
import java.util.List;

public class LeaderboardActivity extends AppCompatActivity {

    RecyclerView recyclerView;
    ProfileAdapter adapter;

    List<Profile> profileList;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_leaderboard);

        profileList = new ArrayList<>();

        recyclerView = (RecyclerView) findViewById(R.id.leaderboard_recycler);
        recyclerView.setHasFixedSize(true);
        recyclerView.setLayoutManager(new LinearLayoutManager(this));

        profileList.add(
                new Profile(
                        "Test Name 1",
                        2,
                        32));
        profileList.add(
                new Profile(
                        "Test Name 3",
                        2,
                        12));
        profileList.add(
                new Profile(
                        "Test Name 22",
                        2,
                        8));
        profileList.add(
                new Profile(
                        "Test Name 66",
                        2,
                        58));

        adapter = new ProfileAdapter(this, profileList);
        recyclerView.setAdapter(adapter);
    }
    public void returnToProfile(View view) {
        Intent myIntent = new Intent(LeaderboardActivity.this, ProfileActivity.class);
        startActivity(myIntent);
    }
}