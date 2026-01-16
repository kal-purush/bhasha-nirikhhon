package com.example.pacmanlike;

import android.view.View;
import android.widget.ImageView;
import android.widget.RadioGroup;
import android.widget.TableLayout;
import android.widget.TableRow;

import androidx.appcompat.app.AppCompatActivity;

import com.google.android.material.tabs.TabLayout;

public class LevelView {
    AppCompatActivity activity;
    GameMap map;

    public LevelView(AppCompatActivity activity, GameMap map){
        this.activity = activity;
        this.map = map;
    }

    public void CreateLevel(){
        // Get table
        // RadioGroup rgp = (RadioGroup) findViewById(R.id.radioGroup);
        TableLayout tbl = (TableLayout) activity.findViewById(R.id.table);


        // Give each table row exactly LevelParser.MAP_SIZE_X children
        for (int i = 0; i < tbl.getChildCount() - 1; i++){
            TableRow row = (TableRow) tbl.getChildAt(i);

            for (int j = 0; j < LevelParser.MAP_SIZE_X; j++){
                ImageView image = new ImageView(activity);
                row.addView(image);

                image.setBackgroundResource(map.map[i][j].drawableId);
                image.setRotation(map.map[i][j].rotation);
                image.getLayoutParams().width = 100;
                image.getLayoutParams().height = 100;
            }
        }

        // Look if there really are LevelParser.MAP_SIZE_Y rows
    }
}