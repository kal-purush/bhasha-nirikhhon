package com.example.imagehandling;

import androidx.appcompat.app.AppCompatActivity;

import android.graphics.Bitmap;
import android.graphics.BitmapFactory;
import android.os.Bundle;
import android.os.StrictMode;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.ImageView;

import java.net.MalformedURLException;
import java.net.URL;

public class MainActivity2 extends AppCompatActivity {
    ImageView imageView;
    EditText editText;
    Button btn;
    URL url;
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main2);


        StrictMode.ThreadPolicy policy = new StrictMode.ThreadPolicy.Builder().permitAll().build();
        StrictMode.setThreadPolicy(policy);
        imageView = findViewById(R.id.imageView2);
        editText = findViewById(R.id.myedittext);
        btn = findViewById(R.id.mybtn);
        btn.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                try {
                    url = new URL("https://static.dw.com/image/57084900_303.jpg");
                    Bitmap bmp = BitmapFactory.decodeStream(url.openConnection().getInputStream());
                    imageView.setImageBitmap(bmp);
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        });


    }
}