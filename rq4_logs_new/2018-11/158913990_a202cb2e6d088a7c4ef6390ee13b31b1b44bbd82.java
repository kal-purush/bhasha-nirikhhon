package com.example.wafa.studentapp;

import android.content.Intent;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.view.MenuItem;
import android.view.View;

public class CourseParent extends AppCompatActivity {

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_course_parent);
        getSupportActionBar().setDisplayShowHomeEnabled(true);
        getSupportActionBar().setDisplayHomeAsUpEnabled(true);
    }
    @Override
    public boolean onOptionsItemSelected(MenuItem item) {
        int id=item.getItemId();
        if(id == android.R.id.home){
            this.finish();
        }
        return super.onOptionsItemSelected(item);
    }
    /// when Parent select the course of their son's go ..

    public void GoToMaterial(View v){
        Intent i = new Intent(this, CourseParentTI473.class);
        startActivity(i);
    }

    public void GoToMaterialCS383(View v){
        Intent i = new Intent(this, CourseParentCS383.class);
        startActivity(i);
    }

    public void GoToMaterialIT443(View v){
        Intent i = new Intent(this, CourseParentIT443.class);
        startActivity(i);
    }

    public void GoToMaterialIT215(View v){
        Intent i = new Intent(this, CourseParentIT215.class);
        startActivity(i);
    }

    public void GoToMaterialIT342(View v){
        Intent i = new Intent(this, CourseParentIT342.class);
        startActivity(i);
    }
    public void GoToMaterialcs214(View v){
        Intent i = new Intent(this, CourseParentCS214.class);
        startActivity(i);
    }
}