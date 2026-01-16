package com.byteshaft.shiurim;

import android.content.Intent;
import android.support.v4.app.Fragment;
import android.support.v4.app.FragmentTransaction;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.view.View;
import android.widget.Button;

public class MenuActivity extends AppCompatActivity implements View.OnClickListener {
    Button mMin;
    Button mShi;
    Button mUpdate;
    Button mContact;
    Button mAds;

    String  cal = "cal";
    String  shi = "shi";
    String  update = "update";
    String  contact = "contact";
    String  ads = "ads";

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_menu);
        mMin = (Button) findViewById(R.id.button_minyanim);
        mShi = (Button) findViewById(R.id.button_shiurim);
        mUpdate = (Button) findViewById(R.id.button_update);
        mContact = (Button) findViewById(R.id.button_contact);
        mAds = (Button) findViewById(R.id.button_ads);

        mMin.setOnClickListener(this);
        mShi.setOnClickListener(this);
        mUpdate.setOnClickListener(this);
        mContact.setOnClickListener(this);
        mAds.setOnClickListener(this);

    }

    @Override
    public void onClick(View v) {
        switch (v.getId()) {
            case R.id.button_minyanim:
                Intent intentOne = new Intent(MenuActivity.this, MainActivity.class);
                intentOne.putExtra("key", cal);
                startActivity(intentOne);
                break;

            case R.id.button_shiurim:
                Intent intentTwo = new Intent(MenuActivity.this, MainActivity.class);
                intentTwo.putExtra("key", shi);
                startActivity(intentTwo);
                break;

            case R.id.button_update:
                Intent intentThree = new Intent(MenuActivity.this, MainActivity.class);
                intentThree.putExtra("key", update);
                startActivity(intentThree);
                break;

            case R.id.button_contact:
                Intent intentFour = new Intent(MenuActivity.this, MainActivity.class);
                intentFour.putExtra("key", contact);
                startActivity(intentFour);
                break;

            case R.id.button_ads:
                Intent intentFive = new Intent(MenuActivity.this, MainActivity.class);
                intentFive.putExtra("key", ads);
                startActivity(intentFive);
                break;
        }
    }
}