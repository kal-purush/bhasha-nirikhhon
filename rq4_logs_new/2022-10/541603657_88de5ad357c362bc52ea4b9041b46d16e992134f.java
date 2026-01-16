package com.aptech.team5.mysound.Activity;

import androidx.appcompat.app.AppCompatActivity;
import androidx.appcompat.widget.Toolbar;
import androidx.recyclerview.widget.GridLayoutManager;
import androidx.recyclerview.widget.RecyclerView;

import android.os.Bundle;
import android.util.Log;
import android.view.View;

import com.aptech.team5.mysound.Adapter.ListPlayListAdapter;
import com.aptech.team5.mysound.Model.PlayList;
import com.aptech.team5.mysound.R;
import com.aptech.team5.mysound.Service.APIService;
import com.aptech.team5.mysound.Service.DataService;

import java.util.ArrayList;
import java.util.List;

import retrofit2.Call;
import retrofit2.Callback;
import retrofit2.Response;

public class ListPlayListActivity extends AppCompatActivity {
    Toolbar toolbar;
    RecyclerView recyclerViewplaylist;
    ListPlayListAdapter listPlayListAdapter;
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_list_play_list);
        Mapping();
        Init();
        GetData();
    }

    private void GetData() {
        DataService dataService = APIService.getService();
        Call<List<PlayList>> callback = dataService.getListPlayList();
        callback.enqueue(new Callback<List<PlayList>>() {
            @Override
            public void onResponse(Call<List<PlayList>> call, Response<List<PlayList>> response) {
                ArrayList<PlayList> arrayPlayList= (ArrayList<PlayList>) response.body();
                listPlayListAdapter = new ListPlayListAdapter(ListPlayListActivity.this,arrayPlayList);
                recyclerViewplaylist.setLayoutManager(new GridLayoutManager(ListPlayListActivity.this,2));
                recyclerViewplaylist.setAdapter(listPlayListAdapter);
            }

            @Override
            public void onFailure(Call<List<PlayList>> call, Throwable t) {

            }
        });
    }

    private void Init() {
        setSupportActionBar(toolbar);
        getSupportActionBar().setDisplayHomeAsUpEnabled(true);
        getSupportActionBar().setTitle("Play Lists");
        toolbar.setTitleTextColor(getResources().getColor(R.color.black));
        toolbar.setNavigationOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                finish();
            }
        });
    }

    private void Mapping() {
        toolbar = findViewById(R.id.toolbarplaylist);
        recyclerViewplaylist = findViewById(R.id.recyclerviewplaylist);
    }
}