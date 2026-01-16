package com.example.nitinmehta.stockticker.activities;

import android.os.Bundle;
import android.support.v7.app.AppCompatActivity;
import android.support.v7.widget.LinearLayoutManager;
import android.support.v7.widget.RecyclerView;
import android.util.Log;

import com.example.nitinmehta.stockticker.adapter.IntraDataAdapter;
import com.example.nitinmehta.stockticker.interfaces.ApiService;
import com.example.nitinmehta.stockticker.R;
import com.example.nitinmehta.stockticker.models.BigData;
import com.example.nitinmehta.stockticker.models.ListItemData;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import butterknife.BindView;
import butterknife.ButterKnife;
import retrofit2.Call;
import retrofit2.Callback;
import retrofit2.Response;
import retrofit2.Retrofit;
import retrofit2.converter.gson.GsonConverterFactory;

public class MainActivity extends AppCompatActivity {

    @BindView(R.id.recycler_view)
    RecyclerView recyclerView;

    private Retrofit retrofit;

    private List<ListItemData> listItemDataList;

    private IntraDataAdapter adapter;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);
        ButterKnife.bind(this);
        loadJson();
    }

    private void loadJson() {
        retrofit = new Retrofit.Builder()
                .baseUrl("https://www.alphavantage.co/")
                .addConverterFactory(GsonConverterFactory.create())
                .build();
        ApiService request = retrofit.create(ApiService.class);
        Call<BigData> call = request.getMyJSON();
        call.enqueue(new Callback<BigData>() {

            @Override
            public void onResponse(Call<BigData> call, Response<BigData> response) {
                BigData jsonResponse = response.body();
                setupAdapter(jsonResponse);
            }


            @Override
            public void onFailure(Call<BigData> call, Throwable t) {
                Log.e("Error",t.getMessage());
            }
        });
}

    public void setupAdapter(BigData json) {
        populateListItemDataList(json);
        recyclerView.setLayoutManager(new LinearLayoutManager(this, LinearLayoutManager.VERTICAL, false));
        adapter = new IntraDataAdapter(listItemDataList);
        recyclerView.setAdapter(adapter);
    }

    private void populateListItemDataList(BigData json) {
        listItemDataList = new ArrayList<>();
        Map<String, Map<String, String>> map = json.getTimeSeriesData();
        Iterator iterator = map.keySet().iterator();
        while (iterator.hasNext()) {
            String title = (String) iterator.next();
            BigData.flattenMapKeysWithTitle(map.get(title), title, listItemDataList);
        }
    }
}