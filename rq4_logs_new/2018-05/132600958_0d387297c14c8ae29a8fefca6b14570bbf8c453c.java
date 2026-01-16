package com.example.restaurant;

import android.app.Activity;
import android.content.Intent;
import android.os.Bundle;
import android.widget.TextView;

public class MenuItemActivity extends Activity {

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_menu_item);
        Intent intent  = getIntent();
        MenuItem item = (MenuItem) intent.getSerializableExtra("clickedItem");
        TextView title = findViewById(R.id.name);
        TextView description = findViewById(R.id.description);
        TextView price = findViewById(R.id.price);
        title.setText(item.getName());
        description.setText(item.getDescription());
    }
}