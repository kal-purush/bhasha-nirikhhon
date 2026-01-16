package com.example.madd_project;

import androidx.appcompat.app.AppCompatActivity;

import android.content.Context;
import android.content.Intent;
import android.os.Bundle;
import android.view.View;

public class Sm_Subject_viwes extends AppCompatActivity {

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_sm_subject_viwes);
    }
    public void gotostudentdash(View view) {
        final Context context = this;
        Intent intent = new Intent(context, Sm_Subject_viwes.class);
        startActivity(intent);
    }
}