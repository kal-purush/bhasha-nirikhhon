package com.example.fruitseason;

import android.content.Intent;
import android.graphics.Color;
import android.os.Bundle;
import android.view.View;
import android.widget.AdapterView;
import android.widget.ArrayAdapter;
import android.widget.EditText;
import android.widget.ListView;
import android.widget.TextView;


import androidx.appcompat.app.AppCompatActivity;

public class MyFruits extends AppCompatActivity {
    ListView listView;
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_fruit_sale);
        listView = findViewById(R.id.listv);

        String[] fruits = {"Pineapple", "Banana", "Strawberry"};
        ArrayAdapter<String> adapter = new ArrayAdapter<>(this, R.layout.fruits_list_sale, R.id.txtFruits, fruits);
        listView.setAdapter(adapter);
        listView.setOnItemClickListener(new AdapterView.OnItemClickListener() {
            @Override
            public void onItemClick(AdapterView<?> adapterView, View view, int position, long id) {
                String selectedItem = (String) adapterView.getItemAtPosition(position);
                Intent myIntent = new Intent(MyFruits.this, FruitsSale.class);
                myIntent.putExtra("fruit", selectedItem);
                startActivity(myIntent);
            }
        } );
    }
}