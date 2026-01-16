package com.example.woof;

import androidx.appcompat.app.AppCompatActivity;

import android.content.Intent;
import android.os.Bundle;
import android.view.View;

import com.example.woof.dogs.addDog;
import com.example.woof.dogs.manageMyDog;
import com.example.woof.dogs.uploadPet;

public class userProfile extends AppCompatActivity {

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_user_profile);
    }

    public void moveToAddDogs(View view){
        Intent intent = new Intent(this, addDog.class);
        startActivity(intent);
    }

    public void moveToMyDogs(View view){
        Intent intent = new Intent(this, manageMyDog.class);
        startActivity(intent);
    }
}