package com.example.appchiasecongthucnauan.activities;

import android.content.Intent;
import android.content.SharedPreferences;
import android.os.Bundle;
import android.widget.EditText;
import android.widget.ImageButton;

import androidx.appcompat.app.AppCompatActivity;
import androidx.recyclerview.widget.LinearLayoutManager;
import androidx.recyclerview.widget.RecyclerView;

import com.example.appchiasecongthucnauan.R;
import com.example.appchiasecongthucnauan.adapters.ConversationAdapter;
import com.example.appchiasecongthucnauan.apis.ApiService;
import com.example.appchiasecongthucnauan.models.ConversationDto;
import com.example.appchiasecongthucnauan.utils.RetrofitClient;

import java.util.ArrayList;
import java.util.List;

import retrofit2.Call;
import retrofit2.Callback;
import retrofit2.Response;

public class ConversationListActivity extends AppCompatActivity {
    private RecyclerView conversationsRecyclerView;
    private ConversationAdapter adapter;
    private ApiService apiService;
    private String token;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_conversation_list);

        setupViews();
        setupRecyclerView();
        loadConversations();
    }

    private void setupViews() {
        ImageButton backButton = findViewById(R.id.btnBack);
        backButton.setOnClickListener(v -> finish());

        EditText searchEditText = findViewById(R.id.searchConversations);
        // Implement search functionality if needed

        SharedPreferences sharedPreferences = getSharedPreferences("MyAppPrefs", MODE_PRIVATE);
        token = sharedPreferences.getString("token", "");
        apiService = RetrofitClient.getInstance().getApiService();
    }

    private void setupRecyclerView() {
        conversationsRecyclerView = findViewById(R.id.conversationsRecyclerView);
        conversationsRecyclerView.setLayoutManager(new LinearLayoutManager(this));
        adapter = new ConversationAdapter(new ArrayList<>(), this::setupForConversationClick);
        conversationsRecyclerView.setAdapter(adapter);
    }
    private void setupForConversationClick(ConversationDto conversationDto){
        Intent intent = new Intent(this, ChatActivity.class);
        SharedPreferences sharedPreferences = getSharedPreferences("MyAppPrefs", MODE_PRIVATE);
        String currentUserId = sharedPreferences.getString("userId", "");
        intent.putExtra("RECIPIENT_ID", conversationDto.getOtherUserId());
        intent.putExtra("RECIPIENT_NAME", conversationDto.getOtherUserName());
        intent.putExtra("CURRENT_USER_ID", currentUserId);
        intent.putExtra("userId", currentUserId);
        startActivity(intent);
    }

    private void loadConversations() {
        apiService.getConversations("Bearer " + token).enqueue(new Callback<List<ConversationDto>>() {
            @Override
            public void onResponse(Call<List<ConversationDto>> call, Response<List<ConversationDto>> response) {
                if (response.isSuccessful() && response.body() != null) {
                    adapter.setConversations(response.body());
                }
            }

            @Override
            public void onFailure(Call<List<ConversationDto>> call, Throwable t) {
                // Handle error
            }
        });
    }
}