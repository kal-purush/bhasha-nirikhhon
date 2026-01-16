package com.example.studysphere;

import android.content.Intent;
import android.os.Bundle;
import android.widget.Button;
import android.widget.TextView;
import android.widget.Toast;

import androidx.appcompat.app.AppCompatActivity;

import com.google.firebase.auth.FirebaseAuth;
import com.google.firebase.auth.FirebaseUser;
import com.google.firebase.firestore.*;

public class HomeActivity extends AppCompatActivity {

    private TextView homeWelcome;
    private Button btnLibrary, btnGroups, btnProfile;
    private FirebaseAuth mAuth;
    private FirebaseFirestore db;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_home);

        mAuth = FirebaseAuth.getInstance();
        db = FirebaseFirestore.getInstance();

        homeWelcome = findViewById(R.id.homeWelcome);
        btnLibrary = findViewById(R.id.btnLibrary);
        btnGroups = findViewById(R.id.btnGroups);
        btnProfile = findViewById(R.id.btnProfile);

        FirebaseUser user = mAuth.getCurrentUser();
        if (user != null) {
            String uid = user.getUid();

            db.collection("users").document(uid)
                    .get()
                    .addOnSuccessListener(documentSnapshot -> {
                        if (documentSnapshot.exists()) {
                            String fullName = documentSnapshot.getString("fullName");
                            homeWelcome.setText("Welcome, " + fullName);
                        } else {
                            homeWelcome.setText("Welcome, user");
                        }
                    })
                    .addOnFailureListener(e -> {
                        homeWelcome.setText("Welcome!");
                        Toast.makeText(HomeActivity.this, "Failed to load name.", Toast.LENGTH_SHORT).show();
                    });
        }

        btnLibrary.setOnClickListener(v -> {
            // TODO: Launch library activity
            // startActivity(new Intent(HomeActivity.this, LibraryActivity.class));
        });

        btnGroups.setOnClickListener(v -> {
            // TODO: Launch study groups activity
            // startActivity(new Intent(HomeActivity.this, GroupsActivity.class));
        });

        btnProfile.setOnClickListener(v -> {
            // TODO: Launch profile/settings activity
            // startActivity(new Intent(HomeActivity.this, ProfileActivity.class));
        });
    }
}