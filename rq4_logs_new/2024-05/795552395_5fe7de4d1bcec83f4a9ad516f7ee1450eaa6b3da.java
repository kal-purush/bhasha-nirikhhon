package com.hyep.movietracker.screens;

import android.os.Bundle;
import android.view.View;
import android.widget.GridLayout;
import android.widget.ImageButton;
import android.widget.Toast;

import androidx.activity.EdgeToEdge;
import androidx.appcompat.app.AppCompatActivity;
import androidx.core.graphics.Insets;
import androidx.core.view.ViewCompat;
import androidx.core.view.WindowInsetsCompat;
import androidx.recyclerview.widget.GridLayoutManager;

import com.hyep.movietracker.R;

public class DetailSpaceScreen extends AppCompatActivity {

    
    private ImageButton imgBtnBack, imgBtnSpaceSetting;
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        EdgeToEdge.enable(this);
        setContentView(R.layout.detail_space_screen);
//        ViewCompat.setOnApplyWindowInsetsListener(findViewById(R.id.main), (v, insets) -> {
//            Insets systemBars = insets.getInsets(WindowInsetsCompat.Type.systemBars());
//            v.setPadding(systemBars.left, systemBars.top, systemBars.right, systemBars.bottom);
//            return insets;
//        });
        imgBtnBack = (ImageButton) findViewById(R.id.imgBtnBack);
        imgBtnSpaceSetting = (ImageButton) findViewById(R.id.imgBtnSpaceSetting); 
        
        imgBtnBack.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                Toast.makeText(DetailSpaceScreen.this, "Back", Toast.LENGTH_SHORT).show();
            }
        });
        
        
        imgBtnSpaceSetting.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                Toast.makeText(DetailSpaceScreen.this, "Setting", Toast.LENGTH_SHORT).show();
            }
        });
        
    }
}