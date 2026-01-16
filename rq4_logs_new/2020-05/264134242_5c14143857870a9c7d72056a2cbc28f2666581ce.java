package com.example.myinfo;

import android.annotation.SuppressLint;
import android.content.Context;
import android.content.Intent;
import android.os.AsyncTask;
import android.os.Build;
import android.os.Bundle;
import android.widget.SearchView;
import android.widget.Toast;
import android.widget.Toolbar;

import androidx.annotation.Nullable;
import androidx.annotation.RequiresApi;
import androidx.appcompat.app.AppCompatActivity;
import androidx.recyclerview.widget.DefaultItemAnimator;
import androidx.recyclerview.widget.LinearLayoutManager;
import androidx.recyclerview.widget.RecyclerView;

import com.example.myinfo.adapter.RecyclerAdapter;
import com.example.myinfo.model.Data;

import java.util.ArrayList;

public class ListActivity extends AppCompatActivity {
    private AppCompatActivity activity = ListActivity.this;
    Context context = ListActivity.this;
    private RecyclerView recyclerView;
    private ArrayList<Data> list;
    private RecyclerAdapter RecyclerAdapter;
    SearchView searchBox;
    private ArrayList<Data> filteredList;

    @RequiresApi(api = Build.VERSION_CODES.LOLLIPOP)
    @Override
    protected void onCreate(@Nullable Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_list);
        Toolbar toolbar = (Toolbar) findViewById(R.id.toolbar);
        setSupportActionBar(toolbar);
        getSupportActionBar().setDisplayHomeAsUpEnabled(true);
        initViews();
        initObjects();

        Intent intentThatStartedThisActivity = getIntent();
        if (intentThatStartedThisActivity.hasExtra("NAME")) {

            //get all needed extras intent

            int id = getIntent().getExtras().getInt("ID");
            String contact = getIntent().getExtras().getString("CONTACT");
            String address = getIntent().getExtras().getString("ADDRESS");



        }else{

            Toast.makeText(this, "No API Data", Toast.LENGTH_SHORT).show();

        }

    }

    private void setSupportActionBar(Toolbar toolbar) {
    }

    /**
     * This method is to initialize views
     */
    private void initViews() {
        recyclerView = (RecyclerView) findViewById(R.id.recyclerView);
    }

    /**
     * This method is to initialize objects to be used
     */
    private void initObjects() {
        list = new ArrayList<>();
        RecyclerAdapter = new RecyclerAdapter(list, this);

        RecyclerView.LayoutManager mLayoutManager = new LinearLayoutManager(getApplicationContext());
        recyclerView.setLayoutManager(mLayoutManager);
        recyclerView.setItemAnimator(new DefaultItemAnimator());
        recyclerView.setHasFixedSize(true);
        recyclerView.setAdapter(RecyclerAdapter);

    }

}