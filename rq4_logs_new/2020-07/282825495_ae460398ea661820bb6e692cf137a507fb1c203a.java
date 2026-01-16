package com.example.instagram;

import androidx.appcompat.app.AppCompatActivity;

import android.content.Intent;
import android.database.Cursor;
import android.graphics.Bitmap;
import android.graphics.BitmapFactory;
import android.os.Bundle;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ImageView;
import android.widget.LinearLayout;
import android.widget.Toast;

public class UserFeedActivity extends AppCompatActivity {

    ImageView imageView;
    LinearLayout linearLayout;

    public void get(String user) {
        Cursor c = MainActivity.mydatabase.rawQuery("select * from login where username = user", null);
        if(c.moveToNext())
        {
            byte[] image = c.getBlob(2);
            Bitmap bmp= BitmapFactory.decodeByteArray(image, 0 , image.length);
            imageView = new ImageView(getApplicationContext());
            imageView.setLayoutParams(new ViewGroup.LayoutParams(
                    ViewGroup.LayoutParams.MATCH_PARENT,
                    ViewGroup.LayoutParams.WRAP_CONTENT
            ));
            imageView.setImageBitmap(bmp);
            linearLayout.addView(imageView);
        }
    }

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_user_feed);

        linearLayout = (LinearLayout) findViewById(R.id.linearLayour);
        Intent intent = getIntent();
        String activeuser = intent.getStringExtra("username");
        setTitle(activeuser+"'s Feed");
        get(activeuser);

    }
}