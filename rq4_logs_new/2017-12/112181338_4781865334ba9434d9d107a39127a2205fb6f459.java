package com.example.mdhvr.memegenerator;

import android.os.Build;
import android.os.Bundle;
import android.support.annotation.RequiresApi;
import android.support.v7.app.AppCompatActivity;
import android.view.View;
import android.view.animation.Animation;
import android.view.animation.AnimationUtils;
import android.widget.ImageView;
import android.widget.Toast;

public class PaintActivity extends AppCompatActivity {

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_paint);
        getSupportActionBar().hide();
        final ImageView left_to_right_imageView= findViewById(R.id.paint_nav_right);
        left_to_right_imageView.setOnClickListener(new View.OnClickListener() {
            @RequiresApi(api = Build.VERSION_CODES.LOLLIPOP)
            @Override
            public void onClick(View view) {

                Animation leftToRight= AnimationUtils.loadAnimation(PaintActivity.this,R.anim.left_to_right);
                Animation RightTOLeft= AnimationUtils.loadAnimation(PaintActivity.this,R.anim.right_to_left);
                if(left_to_right_imageView.getDrawable().getConstantState()==
                        getResources().getDrawable(R.mipmap.navigation_left).getConstantState()){
                    left_to_right_imageView.startAnimation(leftToRight);
                    left_to_right_imageView.setImageResource(R.mipmap.navigation_right);
                    Toast.makeText(PaintActivity.this,"180 to 0",Toast.LENGTH_SHORT).show();
                }
                else if(left_to_right_imageView.getDrawable().getConstantState()
                        ==getResources().getDrawable(R.mipmap.navigation_right).getConstantState()){
                    left_to_right_imageView.startAnimation(RightTOLeft);
                    left_to_right_imageView.setImageResource(R.mipmap.navigation_left);
                    Toast.makeText(PaintActivity.this,"0 to 180",Toast.LENGTH_SHORT).show();
                }
            }
        });

    }
}