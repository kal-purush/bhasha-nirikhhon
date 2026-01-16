package com.example.oguz.whatsaround;

import android.os.Bundle;
import android.support.annotation.Nullable;
import android.support.v4.app.Fragment;
import android.support.v7.app.AppCompatActivity;

/**
 * Created by oguz on 23.05.2017.
 */

public class FirmaActivity extends AppCompatActivity {
    @Override
    protected void onCreate(@Nullable Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_firma);

        Fragment fr = new FirmaFragment();
        this.getSupportFragmentManager().beginTransaction()
                .replace(R.id.fragment_container,fr).addToBackStack("firma").commit();



    }
}