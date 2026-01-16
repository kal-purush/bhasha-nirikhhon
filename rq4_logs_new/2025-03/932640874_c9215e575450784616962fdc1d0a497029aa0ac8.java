package dev.frilly.locket.activities;
import dev.frilly.locket.R;

import android.content.Intent;
import android.os.Bundle;
import android.widget.ArrayAdapter;
import android.widget.ImageView;
import android.widget.Spinner;
import android.widget.TextView;
import android.widget.Toast;

import androidx.annotation.Nullable;
import androidx.appcompat.app.AppCompatActivity;

import com.bumptech.glide.Glide;


import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

public class FriendsPostActivity extends AppCompatActivity {

    private ImageView imageView;
    private TextView messageTextView;
    private TextView posterTextView;
    private TextView postDateTextView;
    private Spinner userFilterSpinner;

    @Override
    protected void onCreate(@Nullable Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.friends_post_screen);

        imageView = findViewById(R.id.imageView);
        messageTextView = findViewById(R.id.message_confirm_input);
        posterTextView = findViewById(R.id.poster);
        postDateTextView = findViewById(R.id.post_date);
        userFilterSpinner = findViewById(R.id.user_filter_spinner);

        setupSpinner();

        // Get data from intent
        Intent intent = getIntent();
        if (intent != null) {
            String imageUrl = intent.getStringExtra("imageUrl");
            String message = intent.getStringExtra("message");
            String username = intent.getStringExtra("username");
            String postTime = intent.getStringExtra("postTime");

            // Debugging logs
            if (imageUrl == null || imageUrl.isEmpty()) {
                Toast.makeText(this, "Image URL is empty!", Toast.LENGTH_SHORT).show();
            } else {
                // Load image using Glide only if URL is valid
                Glide.with(this).load(imageUrl).into(imageView);
            }

            // Set text values
            messageTextView.setText(message != null ? message : "No message");
            posterTextView.setText(username != null ? username : "Unknown");
            postDateTextView.setText(postTime != null ? calculateTimeDifference(postTime) : "Unknown date");
        }
    }

    private String calculateTimeDifference(String postTime) {
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss", Locale.getDefault());
            Date postDate = sdf.parse(postTime);
            if (postDate != null) {
                long diff = new Date().getTime() - postDate.getTime();
                long days = TimeUnit.MILLISECONDS.toDays(diff);

                if (days == 0) {
                    long hours = TimeUnit.MILLISECONDS.toHours(diff);
                    if (hours == 0) {
                        long minutes = TimeUnit.MILLISECONDS.toMinutes(diff);
                        if (minutes == 0) {
                            long seconds = TimeUnit.MILLISECONDS.toSeconds(diff);
                            return seconds + " seconds";
                        }
                        return minutes + " hours";
                    }
                    return hours + " hours";
                }
                return days + " days";
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "Unknown";
    }

    private void setupSpinner() {
        String[] users = {"Everyone", "nthung", "nthung01", "nthung02"};  // Add your users here

        // Use android.R.layout.simple_spinner_item instead of simple_spinner_dropdown_item
        ArrayAdapter<String> adapter = new ArrayAdapter<>(this, R.layout.spinner_item, users);
        adapter.setDropDownViewResource(R.layout.spinner_drop_item);

        userFilterSpinner.setAdapter(adapter);

        // Ensure "Everyone" is selected by default AFTER setting the adapter
        userFilterSpinner.post(() -> userFilterSpinner.setSelection(0, false));
    }
}
