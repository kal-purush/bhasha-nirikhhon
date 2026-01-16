package com.kanyi.bigtuna;

import androidx.appcompat.app.AppCompatActivity;

import android.os.Bundle;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.ImageButton;

public class ProfileEditSellerActivity extends AppCompatActivity {

    private ImageButton backBtn;
    private EditText full_name, phone, address;
    private Button update_button;

    private static final int LOCATION_REQUEST_CODE =100;
    private static final int CAMERA_REQUEST_CODE = 200;
    private static final int STORAGE_REQUEST_CODE = 300;

    private static final int IMAGE_PICK_GALLERY_CODE = 400;
    private static final int IMAGE_PICK_CAMERA_CODE = 500;

    private String[] locationPermission;
    private String[] cameraPermission;
    private String[] storagePermission;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_profile_edit_seller);

        backBtn = findViewById(R.id.backBtn);
        full_name = findViewById(R.id.fullname);
        phone = findViewById(R.id.phone);
        address = findViewById(R.id.address);
        update_button = findViewById(R.id.update_button);

        backBtn.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                onBackPressed();
            }
        });

        update_button.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {

            }
        });

    }
}