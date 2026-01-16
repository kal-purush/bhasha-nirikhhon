package com.example.travelappfragment;

import static android.content.ContentValues.TAG;

import androidx.activity.result.ActivityResult;
import androidx.activity.result.ActivityResultCallback;
import androidx.activity.result.ActivityResultLauncher;
import androidx.activity.result.contract.ActivityResultContracts;
import androidx.annotation.Nullable;
import androidx.appcompat.app.AppCompatActivity;

import android.app.Activity;
import android.content.Intent;
import android.graphics.Bitmap;
import android.net.Uri;
import android.os.Bundle;
import android.provider.MediaStore;
import android.util.Log;
import android.view.View;
import android.widget.Toast;

import com.example.travelappfragment.databinding.ActivityAddplaceBinding;
import com.example.travelappfragment.databinding.ActivityLoginBinding;

public class AddplaceActivity extends AppCompatActivity {

    ActivityAddplaceBinding binding;

    MapsActivity mapsActivity;

    private Double latitude;
    private Double longtitude;
    private String address;


    DatabaseHelper databaseHelper;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        binding = ActivityAddplaceBinding.inflate(getLayoutInflater());
        setContentView(binding.getRoot());

        databaseHelper = new DatabaseHelper(this);


        Intent intent = getIntent();
        binding.backBtn.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                finish();
            }
        });


        binding.saveBtn.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view){
                String title = binding.title.getText().toString();
                String address = binding.address.getText().toString();
                String time = binding.time.getText().toString();
                String price = binding.price.getText().toString();
                String describe = binding.describe.getText().toString();
                if(title.equals("")||address.equals("")||time.equals("")||price.equals("")||
                        describe.equals(""))
                    Toast.makeText(AddplaceActivity.this, "All fields are mandatory", Toast.LENGTH_SHORT).show();
                else{
                    Boolean insert = databaseHelper.insertPlace(title,  address,  time,  price,  describe);
                    if(insert == true) {
                        Toast.makeText(AddplaceActivity.this, "thanh cong", Toast.LENGTH_SHORT).show();
                        finish();
                    }else {
                        Toast.makeText(AddplaceActivity.this, "thất bại", Toast.LENGTH_SHORT).show();
                    }
                }
            }
        });
        binding.address.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                Intent intent = new Intent(AddplaceActivity.this, MapsActivity.class);
                mapsActivityResultLauncher.launch(intent);


            }
        });
    }

    private ActivityResultLauncher<Intent> mapsActivityResultLauncher = registerForActivityResult(
            new ActivityResultContracts.StartActivityForResult(),
            new ActivityResultCallback<ActivityResult>() {
                @Override
                public void onActivityResult(ActivityResult result) {
                    Log.d(TAG, "onActivityResult: ");

                    if(result.getResultCode() == Activity.RESULT_OK) {
                        Intent data = result.getData();

                        if(data != null) {
                            latitude = data.getDoubleExtra("latitude", 0.0);
                            longtitude = data.getDoubleExtra("longtitude", 0.0);
                            address = data.getStringExtra("address");

                            Log.d(TAG, "onActivityResult: latitude: "+ latitude);
                            Log.d(TAG, "onActivityResult: longtitude: "+ longtitude);
                            Log.d(TAG, "onActivityResult: address: "+ address);

                            binding.address.setText(address);
                        } else {
                            Log.d(TAG, "onActivityResult: cancelled: ");
                        }
                    }
                }
            }
    );


}