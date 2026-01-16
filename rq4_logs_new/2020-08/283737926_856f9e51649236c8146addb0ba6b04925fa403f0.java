package com.example.geekbrainsandroidweather;

import androidx.appcompat.app.AppCompatActivity;
import androidx.recyclerview.widget.DividerItemDecoration;
import androidx.recyclerview.widget.LinearLayoutManager;
import androidx.recyclerview.widget.RecyclerView;

import android.content.Intent;
import android.os.Bundle;
import android.util.TypedValue;
import android.view.View;
import android.widget.Button;
import android.widget.TextView;

import com.example.geekbrainsandroidweather.model.weather.Main;
import com.example.geekbrainsandroidweather.recycler_views.IRVOnItemClick;
import com.example.geekbrainsandroidweather.recycler_views.RecyclerDataAdapter;
import com.google.android.material.snackbar.Snackbar;
import com.google.android.material.textfield.TextInputEditText;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;

import static com.example.geekbrainsandroidweather.fragments.CitiesFragment.cities;

public class AddCityActivity extends AppCompatActivity implements IRVOnItemClick {
    private TextInputEditText addCityInput;
    private RecyclerView recyclerCitiesView;
    private Button addCityBtn;
    private RecyclerDataAdapter recyclerDataAdapter;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_add_city);
        init();
        setupRecyclerView();
        setOnAddCityBtnClickBehaviour();
    }

    private void setOnAddCityBtnClickBehaviour() {
        addCityBtn.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                String inputText = addCityInput.getText().toString();
                if (!inputText.equals("")) {
                    if (!cities.contains(inputText)) {
                        cities.add(inputText);
                        setupRecyclerView();
                        addCityInput.getText().clear();
                    } else {
                        Snackbar.make(view, R.string.snackbar_sity_already_added, Snackbar.LENGTH_LONG).show();
                    }
                } else {
                }
            }
        });
    }

    @Override
    public void onBackPressed() {
        Intent intent = new Intent();
        intent.setFlags(Intent.FLAG_ACTIVITY_CLEAR_TOP);
        intent.setClass(getApplicationContext(), MainActivity.class);
        startActivity(intent);
    }

    private void init() {
        addCityInput = findViewById(R.id.addCityInput);
        recyclerCitiesView = findViewById(R.id.recyclerCitiesView);
        addCityBtn = findViewById(R.id.addCityBtn);
    }

    private void setupRecyclerView() {
        LinearLayoutManager linearLayoutManager = new LinearLayoutManager(getApplicationContext());
        DividerItemDecoration decorator = new DividerItemDecoration(getApplicationContext(),
                LinearLayoutManager.VERTICAL);
        recyclerDataAdapter = new RecyclerDataAdapter(cities, this, this);
        recyclerCitiesView.setLayoutManager(linearLayoutManager);
        recyclerCitiesView.addItemDecoration(decorator);
        recyclerCitiesView.setAdapter(recyclerDataAdapter);
    }

    @Override
    public void onItemClicked(View view, int position) {
    }

    @Override
    public void onItemLongPressed(View view) {
    }

    @Override
    public void changeItem(TextView view) {
    }


}