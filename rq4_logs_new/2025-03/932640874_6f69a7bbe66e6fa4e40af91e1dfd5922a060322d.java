package dev.frilly.locket.activities;

import android.os.Bundle;
import android.util.Log;
import android.view.View;
import android.widget.ImageView;

import androidx.activity.OnBackPressedCallback;
import androidx.annotation.NonNull;
import androidx.appcompat.app.AppCompatActivity;
import androidx.recyclerview.widget.LinearLayoutManager;
import androidx.recyclerview.widget.RecyclerView;

import com.firebase.ui.firestore.FirestoreRecyclerOptions;
import com.google.firebase.firestore.FirebaseFirestore;
import com.google.firebase.firestore.Query;

import dev.frilly.locket.R;
import dev.frilly.locket.adapter.ListUserApdater;
import dev.frilly.locket.model.User;

public class MessengerActivity extends AppCompatActivity {
    private static final String TAG = "MessengerActivity";
    private RecyclerView recyclerView;
    private ListUserApdater userAdapter;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.list_mess_user);

        recyclerView = findViewById(R.id.mess_user_recycler_view);
        recyclerView.setLayoutManager(new LinearLayoutManager(this));
        recyclerView.setItemAnimator(null); // Disable animations to prevent crashes during dataset changes

        createAdapter();

        // Xử lý sự kiện khi bấm vào nút back
        ImageView backBtn = findViewById(R.id.back_btn);
        backBtn.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                finish(); // Đóng Activity hiện tại
            }
        });

        // Xử lý khi bấm nút back trên điện thoại
        getOnBackPressedDispatcher().addCallback(this, new OnBackPressedCallback(true) {
            @Override
            public void handleOnBackPressed() {
                finish(); // Đóng Activity
            }
        });
    }

    private void createAdapter() {
        // Truy vấn Firestore để lấy danh sách user
        Query query = FirebaseFirestore.getInstance()
                .collection("users")
                .orderBy("username");

        FirestoreRecyclerOptions<User> options =
                new FirestoreRecyclerOptions.Builder<User>()
                        .setQuery(query, User.class)
                        .build();

        userAdapter = new ListUserApdater(options);
        userAdapter.setHasStableIds(true); // Enable stable IDs to help RecyclerView track items
        recyclerView.setAdapter(userAdapter);
    }

    @Override
    protected void onStart() {
        super.onStart();
        startListening();
    }

    @Override
    protected void onResume() {
        super.onResume();
        // Ensure the adapter is listening when activity is resumed
        startListening();
    }

    private void startListening() {
        if (userAdapter != null) {
            try {
                userAdapter.startListening();
            } catch (Exception e) {
                Log.e(TAG, "Error starting adapter listener", e);
                // If there's an error, recreate the adapter
                createAdapter();
                userAdapter.startListening();
            }
        }
    }

    @Override
    protected void onStop() {
        super.onStop();
        if (userAdapter != null) {
            userAdapter.stopListening();
        }
    }

    @Override
    protected void onDestroy() {
        super.onDestroy();
        // Ensure adapter is not listening when activity is destroyed
        if (userAdapter != null) {
            userAdapter.stopListening();
        }
    }
}