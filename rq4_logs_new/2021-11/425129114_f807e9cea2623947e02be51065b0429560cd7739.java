package com.example.myapplication;

import androidx.appcompat.app.AppCompatActivity;

import android.content.Intent;
import android.os.Bundle;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;

import java.util.ArrayList;
import java.util.List;

public class AddTask extends AppCompatActivity {

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_add_task);

        Button btn = findViewById(R.id.btn_add_task);

        btn.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {

                EditText name_task = findViewById(R.id.register_task_name);
                EditText description_task =findViewById(R.id.register_description_task);

                String name_t = name_task.getText().toString();
                String description_t = description_task.getText().toString();

                List<List_element> elements = new ArrayList<>();

                elements.add(new List_element("black", name_t, description_t, "Activo", "Alto"));

                Task add = new Task();
                add.add_task(elements);

                Intent browse = new Intent(AddTask.this, Task.class);
                startActivity(browse);
            }
        });
    }
}