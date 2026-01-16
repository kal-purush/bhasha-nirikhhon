package com.hotsauce.meem;

import android.content.Intent;
import android.net.Uri;
import android.os.Bundle;
import android.util.Log;
import android.view.MotionEvent;
import android.view.View;
import android.view.Window;
import android.view.WindowManager;
import android.widget.ImageView;
import androidx.appcompat.app.AppCompatActivity;

public class CreateTemplateActivity extends AppCompatActivity implements View.OnTouchListener {
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        //Remove title bar
        this.requestWindowFeature(Window.FEATURE_NO_TITLE);
        //Remove notification bar
        this.getWindow().setFlags(WindowManager.LayoutParams.FLAG_FULLSCREEN, WindowManager.LayoutParams.FLAG_FULLSCREEN);

        setContentView(R.layout.activity_create_template);

        Intent intent = getIntent();
        Uri imageUri = Uri.parse(intent.getStringExtra("imageUri"));
        ImageView image = findViewById(R.id.image);
        image.setImageURI(imageUri);
        image.setOnTouchListener(this);
    }

    public boolean onTouch (View v, MotionEvent ev) {
        final int action = ev.getAction();
        final int evX = (int) ev.getX();
        final int evY = (int) ev.getY();
        switch (action) {
            case MotionEvent.ACTION_DOWN :
                // TODO
                break;
            case MotionEvent.ACTION_UP :
                // TODO
                break;
        }
        return true;
    }
}