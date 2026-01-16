package com.venus.assistant;

import android.databinding.DataBindingUtil;
import android.databinding.ViewDataBinding;
import android.os.Bundle;
import android.support.annotation.Nullable;
import android.support.v7.app.ActionBar;
import android.support.v7.app.AppCompatActivity;

import com.venus.assistant.databinding.ActivityChooseareaBinding;

public class ChooseAreaActivity extends AppCompatActivity {

    private ActivityChooseareaBinding dataBinding;


    @Override
    protected void onCreate(@Nullable Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        dataBinding = DataBindingUtil.setContentView(this, R.layout.activity_choosearea);

        // setContentView(R.layout.activity_choosearea);
        ActionBar actionBar = getSupportActionBar();
        if (actionBar != null) {
            //actionBar.hide();
        }


    }
}