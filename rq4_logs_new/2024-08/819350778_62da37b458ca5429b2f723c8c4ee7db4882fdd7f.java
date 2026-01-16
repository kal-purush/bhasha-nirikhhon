package com.app.Regional_News;

import android.content.SharedPreferences;
import android.os.Bundle;
import android.util.Log;
import android.view.KeyEvent;
import android.view.MenuItem;
import android.view.View;
import android.widget.LinearLayout;
import android.widget.ProgressBar;
import android.widget.Toast;

import androidx.activity.EdgeToEdge;
import androidx.appcompat.app.AppCompatActivity;
import androidx.core.graphics.Insets;
import androidx.core.view.ViewCompat;
import androidx.core.view.WindowInsetsCompat;
import androidx.recyclerview.widget.DividerItemDecoration;
import androidx.recyclerview.widget.LinearLayoutManager;
import androidx.recyclerview.widget.RecyclerView;
import androidx.swiperefreshlayout.widget.SwipeRefreshLayout;

import com.app.Regional_News.adapter.NewslistAdapter;
import com.app.Regional_News.adapter.SavednewsAdapater;
import com.app.Regional_News.data.News_listfetch_listdata;
import com.app.Regional_News.data.Saved_news_data;
import com.app.Regional_News.data.Saved_news_datalist;
import com.app.Regional_News.extra.BaseApiService;
import com.app.Regional_News.extra.ItemOffsetDecoration;
import com.app.Regional_News.extra.NetworkUtils;
import com.app.Regional_News.extra.UtilsApi;

import java.util.ArrayList;
import java.util.Map;
import android.content.SharedPreferences;
import android.content.Context;

import retrofit2.Call;
import retrofit2.Callback;
import retrofit2.Response;

public class SavedNewsActivity extends AppCompatActivity {

    private SharedPreferences sharedPreferences;
    public RecyclerView recyclerView;
    private ProgressBar progressBar;
    private LinearLayout lyt_not_found;
    BaseApiService mApiService;
    SavednewsAdapater adapter;
    private SwipeRefreshLayout swipeRefreshLayout;
    private ArrayList<Saved_news_datalist> savedNewsList = new ArrayList<>(); // List to store all saved news

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_saved_news);

        // USE FOR DISPLAY SYSTEM INBUILT BACK NAVIGATION ARROW
        getSupportActionBar().setDisplayHomeAsUpEnabled(true);
        // Set the toolbar title
        getSupportActionBar().setTitle("Saved News");
        // Initialize SharedPreferences
        sharedPreferences = getSharedPreferences("favorites", Context.MODE_PRIVATE);

        mApiService = UtilsApi.getAPIService();
        swipeRefreshLayout = findViewById(R.id.swipeRefreshLayout);
        lyt_not_found = findViewById(R.id.lyt_not_found);
        progressBar = findViewById(R.id.progressBar);
        recyclerView = findViewById(R.id.recyclerView);
        recyclerView.setHasFixedSize(true);
        LinearLayoutManager layoutManager = new LinearLayoutManager(this);
        recyclerView.setLayoutManager(layoutManager); // set LayoutManager to RecyclerView

        ItemOffsetDecoration itemDecoration = new ItemOffsetDecoration(this, R.dimen.item_offset);
        recyclerView.addItemDecoration(itemDecoration);
        recyclerView.addItemDecoration(new DividerItemDecoration(this, LinearLayout.VERTICAL));

        swipeRefreshLayout.setOnRefreshListener(new SwipeRefreshLayout.OnRefreshListener() {
            @Override
            public void onRefresh() {
                savedNewsList.clear(); // Clear the list before reloading
                loadSavedNews();
            }
        });

        if (NetworkUtils.isConnected(this)) {
            showProgress(true);
            loadSavedNews();
        } else {
            Toast.makeText(this, getString(R.string.conne_msg1), Toast.LENGTH_SHORT).show();
        }
    }

    private void loadSavedNews() {
        // Fetch saved news from SharedPreferences
        for (Map.Entry<String, ?> entry : sharedPreferences.getAll().entrySet()) {
            if (entry.getValue() instanceof Boolean && (Boolean) entry.getValue()) {
                String newsId = entry.getKey();
                getdata(newsId);
            }
        }
    }

    private void getdata(String newsId) {
        mApiService.rnSavedNewslistRequest("saved_news", newsId)
                .enqueue(new Callback<Saved_news_data>() {
                    @Override
                    public void onResponse(Call<Saved_news_data> call, Response<Saved_news_data> response) {
                        swipeRefreshLayout.setRefreshing(false);
                        if (response.isSuccessful()) {
                            showProgress(false);
                            Saved_news_data savedData = response.body();
                            if (savedData.getStatus().equals("1")) {
                                savedNewsList.addAll(savedData.getSaved_news_datalist()); // Add fetched data to the list
                                displayData(savedNewsList);
                            } else {
                                Toast.makeText(SavedNewsActivity.this, savedData.getMsg(), Toast.LENGTH_SHORT).show();
                            }
                        } else {
                            showProgress(false);
                        }
                    }

                    @Override
                    public void onFailure(Call<Saved_news_data> call, Throwable t) {
                        swipeRefreshLayout.setRefreshing(false);
                        showProgress(false);
                    }
                });
    }

    private void displayData(ArrayList<Saved_news_datalist> newsList) {
        if (adapter == null) {
            adapter = new SavednewsAdapater(this, newsList);
            recyclerView.setAdapter(adapter);
        } else {
            adapter.notifyDataSetChanged();
        }

        if (adapter.getItemCount() == 0) {
            lyt_not_found.setVisibility(View.VISIBLE);
        } else {
            lyt_not_found.setVisibility(View.GONE);
        }
    }

    private void showProgress(boolean show) {
        if (show) {
            progressBar.setVisibility(View.VISIBLE);
            recyclerView.setVisibility(View.GONE);
            lyt_not_found.setVisibility(View.GONE);
        } else {
            progressBar.setVisibility(View.GONE);
            recyclerView.setVisibility(View.VISIBLE);
        }
    }

    @Override
    public boolean onOptionsItemSelected(MenuItem item) {
        if (item.getItemId() == android.R.id.home) {
            finish();
            return true;
        }
        return super.onOptionsItemSelected(item);
    }
}