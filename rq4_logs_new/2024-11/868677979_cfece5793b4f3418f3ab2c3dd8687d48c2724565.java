package com.tutorial.travel.Activity;

import android.os.Bundle;
import android.text.TextUtils;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.Spinner;
import android.widget.Toast;

import androidx.annotation.Nullable;
import androidx.appcompat.app.AppCompatActivity;

import com.tutorial.travel.R;

public class CreatePopularActivity extends AppCompatActivity {

    private EditText placeNameEditText;
    private EditText locationEditText;
    private Spinner categorySpinner;
    private EditText shortDescriptionEditText;
    private EditText detailedDescriptionEditText;
    private Button uploadImageButton;
    private Button saveButton;

    @Override
    protected void onCreate(@Nullable Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_create_popular);

        // Initialize views
        placeNameEditText = findViewById(R.id.placeNameEditText);
        locationEditText = findViewById(R.id.locationEditText);
        categorySpinner = findViewById(R.id.categorySpinner);
        shortDescriptionEditText = findViewById(R.id.shortDescriptionEditText);
        detailedDescriptionEditText = findViewById(R.id.detailedDescriptionEditText);
        uploadImageButton = findViewById(R.id.uploadImageButton);
        saveButton = findViewById(R.id.saveButton);

        // Set listeners
        saveButton.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                if (validateForm()) {
                    submitForm();
                }
            }
        });

        uploadImageButton.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                // Code to open image picker
                Toast.makeText(CreatePopularActivity.this, "Image picker coming soon", Toast.LENGTH_SHORT).show();
            }
        });
    }

    /**
     * Validates the form inputs
     *
     * @return true if all inputs are valid, false otherwise
     */
    private boolean validateForm() {
        if (TextUtils.isEmpty(placeNameEditText.getText().toString())) {
            placeNameEditText.setError("Place name is required");
            return false;
        }
        if (TextUtils.isEmpty(locationEditText.getText().toString())) {
            locationEditText.setError("Location is required");
            return false;
        }
        if (TextUtils.isEmpty(shortDescriptionEditText.getText().toString())) {
            shortDescriptionEditText.setError("Short description is required");
            return false;
        }
        if (TextUtils.isEmpty(detailedDescriptionEditText.getText().toString())) {
            detailedDescriptionEditText.setError("Detailed description is required");
            return false;
        }
        return true;
    }

    /**
     * Submits the form after validation
     */
    private void submitForm() {
        String placeName = placeNameEditText.getText().toString();
        String location = locationEditText.getText().toString();
        String category = categorySpinner.getSelectedItem().toString();
        String shortDescription = shortDescriptionEditText.getText().toString();
        String detailedDescription = detailedDescriptionEditText.getText().toString();

        // Show a Toast message as a placeholder for actual submission logic
        Toast.makeText(this, "Form submitted successfully!\n" +
                        "Place Name: " + placeName + "\n" +
                        "Location: " + location + "\n" +
                        "Category: " + category + "\n" +
                        "Short Description: " + shortDescription + "\n" +
                        "Detailed Description: " + detailedDescription,
                Toast.LENGTH_LONG).show();

        // Add actual submission logic here (e.g., sending data to a server or saving to a database)
    }
}
