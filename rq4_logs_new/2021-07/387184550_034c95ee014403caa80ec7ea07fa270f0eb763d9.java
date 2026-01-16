package it.units.placesapp;

import androidx.annotation.NonNull;
import androidx.appcompat.app.AppCompatActivity;

import android.os.Bundle;
import android.view.View;
import android.widget.Toast;

import com.google.firebase.auth.FirebaseAuth;
import com.google.firebase.auth.FirebaseUser;
import com.google.firebase.database.DataSnapshot;
import com.google.firebase.database.DatabaseError;
import com.google.firebase.database.DatabaseReference;
import com.google.firebase.database.FirebaseDatabase;
import com.google.firebase.database.ValueEventListener;

import java.util.HashMap;

import it.units.placesapp.databinding.ActivityUploadReviewBinding;

public class UploadReviewActivity extends AppCompatActivity {
    private ActivityUploadReviewBinding binding;
    double latitude;
    double longitude;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        binding = ActivityUploadReviewBinding.inflate(getLayoutInflater());
        setContentView(binding.getRoot());

        binding.back.setOnClickListener(backOnButtonListener);
        binding.submit.setOnClickListener(postNewReviewListener);

        latitude = getIntent().getDoubleExtra("latitude", 0);
        longitude = getIntent().getDoubleExtra("longitude", 0);
    }

    View.OnClickListener backOnButtonListener = v -> finish();

    View.OnClickListener postNewReviewListener = v -> {
        final String review = binding.reviewField.getText().toString();
        if (review.isEmpty()) {
            Toast.makeText(UploadReviewActivity.this, R.string.emptyNotAllowed, Toast.LENGTH_LONG).show();
        } else {
            HashMap<String, String> data = new HashMap<>();
            String saveLatitude = Double.toString(latitude).replace(".", "");
            String saveLongitude = Double.toString(longitude).replace(".", "");
            data.put("review", review);
            FirebaseDatabase.getInstance().getReference("review").child(saveLatitude + "_" + saveLongitude).push().setValue(data);
            binding.reviewField.getText().clear();
            Toast.makeText(UploadReviewActivity.this, "Recensione caricata", Toast.LENGTH_LONG).show();
        }
    };

}