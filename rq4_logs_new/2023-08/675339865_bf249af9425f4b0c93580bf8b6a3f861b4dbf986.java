package com.home.homify;

import android.content.Intent;
import android.os.Bundle;
import android.view.View;
import android.widget.TableLayout;

import androidx.appcompat.app.AppCompatActivity;

import com.home.homify.databinding.ActivityCookingBinding;

public class CategoryCooking extends AppCompatActivity {

    private ActivityCookingBinding binding;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        binding = ActivityCookingBinding.inflate(getLayoutInflater());
        View view = binding.getRoot();
        setContentView(view);

        TableLayout tableLayout = findViewById(R.id.tableLayout);
        DatabaseUtils.populateTableLayout(this, tableLayout, "food", "Sütés/főzés");

        binding.addNewItemButton.setOnClickListener(v-> {
            Intent intent = new Intent(CategoryCooking.this, AddItemActivity.class);
            startActivity(intent);
        });
    }
}