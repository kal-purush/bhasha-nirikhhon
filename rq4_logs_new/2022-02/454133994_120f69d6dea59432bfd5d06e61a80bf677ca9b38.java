package com.codewithArdents.dysgraphia;

import androidx.appcompat.app.AppCompatActivity;
import androidx.recyclerview.widget.GridLayoutManager;
import androidx.recyclerview.widget.RecyclerView;

import android.graphics.Color;
import android.os.Bundle;

import java.util.ArrayList;

public class NumberSelection extends AppCompatActivity {

    RecyclerView rv;
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_number_selection);
        rv = findViewById(R.id.num_recycler);
        ArrayList<Integer> list = new ArrayList<>();
        ArrayList<Integer> color = new ArrayList<>();
        list.add(R.drawable.num_0);
        list.add(R.drawable.num_1);
        list.add(R.drawable.num_2);
        list.add(R.drawable.num_3);
        list.add(R.drawable.num_4);
        list.add(R.drawable.num_5);
        list.add(R.drawable.num_6);
        list.add(R.drawable.num_7);
        list.add(R.drawable.num_8);
        list.add(R.drawable.num_9);
        String[] str = getResources().getStringArray(R.array.num_colors);
        for(int i=0;i<str.length;i++)
        {
            color.add(Color.parseColor(str[i]));
        }

        LetterCard_Adapter adapter = new LetterCard_Adapter(this,list,color);
        rv.setLayoutManager(new GridLayoutManager(getApplicationContext(),2));
        rv.setAdapter(adapter);
    }
}