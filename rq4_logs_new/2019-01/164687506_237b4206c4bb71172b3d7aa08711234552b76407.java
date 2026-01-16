package com.bassett.health_tracker_app;

import android.content.Intent;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.view.View;
import android.widget.Button;
import android.widget.TextView;

public class ExerciseDiary extends AppCompatActivity {

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_exercise_diary);

        Intent intent = getIntent();

        Button button = findViewById(R.id.addExercise);
        button.setOnClickListener(new View.OnClickListener() {
            public void onClick(View v) {
                // Do something in response to button click
            }
        });
    }

    public void addExercise(View v) {
        TextView textViewName = findViewById(R.id.nameTextView);
        TextView textViewDescription = findViewById(R.id.descriptionTextView);

        NoteRepository noteRepository = new NoteRepository(getApplicationContext());
        String title = textViewName.getText().toString();
        String description = textViewDescription.getText().toString();
        noteRepository.insertTask(title, description);
    }


}