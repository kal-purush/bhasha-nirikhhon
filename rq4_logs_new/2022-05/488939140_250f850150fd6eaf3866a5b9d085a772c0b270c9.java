package com.example.opsc_7311_poe;

import androidx.appcompat.app.AppCompatActivity;

import android.content.Intent;
import android.os.Bundle;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.ImageView;
import android.widget.ListView;

import java.util.List;

public class AddItemScreen extends AppCompatActivity {

    public String Date;
    public String Description;
    public int image;
    int clicks = 0;

    EditText inputDate;
    EditText inputDescription;

    Button applyButton;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_add_item_screen);

        inputDate =  findViewById(R.id.DateInput);
        inputDescription =  findViewById(R.id.DescriptionInput);
        applyButton = findViewById(R.id.ApplyButton);

        applyButton.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                if(inputDate != null)
                {
                    ListItemScreen listItemScreen = new ListItemScreen();
                    clicks++;
                    ListItemScreen.AmountofDate = clicks;
                    listItemScreen.ItemName[clicks] = inputDate.toString();
                    listItemScreen.itemDescription[clicks] = inputDescription.toString();
                    listItemScreen.itemImages[clicks] = 1;
                    openListitemScreen();
                }


            }
        });


    }
    public void openListitemScreen()
    {
        Intent intent = new Intent(this,ListItemScreen.class);
        startActivity(intent);
    }
}