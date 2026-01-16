package com.group4sweng.scranplan.Drawing;

import android.app.Activity;
import android.content.Intent;
import android.graphics.Color;
import android.os.Bundle;
import android.util.DisplayMetrics;
import android.util.Log;
import android.view.ScaleGestureDetector;
import android.view.View;
import android.widget.Button;
import android.widget.LinearLayout;
import android.widget.Toast;

import androidx.appcompat.app.AppCompatActivity;

import com.flask.colorpicker.ColorPickerView;
import com.flask.colorpicker.builder.ColorPickerDialogBuilder;
import com.group4sweng.scranplan.R;

public class LayoutCreator extends AppCompatActivity {

    private GraphicsView mGraphicsView;

    private Integer screenWidth;
    private Integer screenHeight;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.graphics);

        DisplayMetrics displayMetrics = new DisplayMetrics();
        getWindowManager().getDefaultDisplay().getMetrics(displayMetrics);
        screenWidth = displayMetrics.widthPixels;
        screenHeight = displayMetrics.heightPixels;

        GraphicsView mGraphicsView = new GraphicsView(this, screenHeight, screenWidth, true);
        LinearLayout linearLayout = findViewById(R.id.graphicsLayout);
        linearLayout.addView(mGraphicsView);

        findViewById(R.id.graphicsOval).setOnClickListener(new View.OnClickListener()
        {
            @Override
            public void onClick(View v)
            {
                Rectangle rect = new Rectangle(screenWidth / 2, screenHeight / 2, screenWidth / 4, screenHeight / 4);

                mGraphicsView.addOval(new Rectangle(screenWidth / 2, screenHeight / 2, screenWidth / 4, screenHeight / 4), 0, 0);
            }
        });

        findViewById(R.id.graphicsRect).setOnClickListener(new View.OnClickListener()
        {
            @Override
            public void onClick(View v)
            {
                //function to do something if pressed
                mGraphicsView.addRectangle(new Rectangle(screenWidth / 2, screenHeight / 2, screenWidth / 4, screenHeight / 4), 0, 0);

            }
        });

        findViewById(R.id.graphicsTri).setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                mGraphicsView.addTriangle(new Triangle(screenWidth / 2, screenHeight / 2, 200f),0 , 0);
            }
        });

        findViewById(R.id.graphicsDel).setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                mGraphicsView.deleteSelected();
            }
        });

        Button colourButton = findViewById(R.id.graphicsCol);
        colourButton.setOnClickListener(view -> ColorPickerDialogBuilder
                .with(view.getContext())
                .setTitle("Choose color")
                .initialColor(Color.WHITE)
                .wheelType(ColorPickerView.WHEEL_TYPE.FLOWER)
                .density(12)
                .setOnColorSelectedListener(selectedColor -> Toast.makeText(getApplicationContext(),
                        "onColorSelected: 0x" + Integer.toHexString(selectedColor),
                        Toast.LENGTH_SHORT).show())
                .setPositiveButton("ok", (dialog, selectedColor, allColors) -> {
                    colourButton.setBackgroundColor(selectedColor);
                    mGraphicsView.setColour(selectedColor); })
                .setNegativeButton("cancel", (dialog, which) -> {})
                .build()
                .show()
        );

        findViewById(R.id.graphicsSubmit).setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                Intent graphics = new Intent();
                graphics.putExtra("shapes", mGraphicsView.getShapes());
                graphics.putExtra("triangles", mGraphicsView.getTriangles());
                setResult(Activity.RESULT_OK, graphics);
                finish();
            }
        });

//        findViewById(R.id.).setOnClickListener(new View.OnClickListener()
//        {
//            @Override
//            public void onClick(View v)
//            {
//                mGraphicsView.updateLine();
//            }
//        });
//
//        findViewById(R.id.btn_fill).setOnClickListener(new View.OnClickListener() {
//            @Override
//            public void onClick(View v)
//            {
//                mGraphicsView.hollow_fill(0);
//            }
//        });
//
//        findViewById(R.id.btn_hollow).setOnClickListener(new View.OnClickListener() {
//            @Override
//            public void onClick(View v)
//            {
//                mGraphicsView.hollow_fill(1);
//            }
//        });
//
//        findViewById(R.id.btn_bigger).setOnClickListener(new View.OnClickListener()
//        {
//            @Override
//            public void onClick(View v)
//            {
//                mGraphicsView.modify_size_selected(0);
//            }
//        });
//
//        findViewById(R.id.btn_smaller).setOnClickListener(new View.OnClickListener()
//        {
//            @Override
//            public void onClick(View v)
//            {
//                mGraphicsView.modify_size_selected(1);
//            }
//        });
//
//        findViewById(R.id.btn_add_triangle).setOnClickListener(new View.OnClickListener()
//        {
//            @Override
//            public void onClick(View v)
//            {
//                mGraphicsView.new_triangle();
//            }
//        });
//
//        findViewById(R.id.btn_delete_triangle).setOnClickListener(new View.OnClickListener()
//        {
//            @Override
//            public void onClick(View v)
//            {
//                mGraphicsView.delete_triangle();
//            }
//        });
    }
}