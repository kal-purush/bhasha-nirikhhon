package com.example.todoapp;

import androidx.appcompat.app.AppCompatActivity;

import android.content.Context;
import android.content.Intent;
import android.os.Bundle;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.ImageButton;
import android.widget.Toast;

public class EditNote extends AppCompatActivity{
    EditText editTitle, editDescription;
    Button editNote, cancelBtn;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_edit_note);

        editTitle = findViewById(R.id.edit_title_note);
        editDescription = findViewById(R.id.edit_description_note);
        editNote = findViewById(R.id.edit_note_info);
        cancelBtn = findViewById(R.id.btnCancel);

        //Get info from Intent and put it in EditNote fields
        Intent intent = getIntent();
        String title = intent.getStringExtra("title");
        String description = intent.getStringExtra("description");
        editTitle.setText(title);
        editDescription.setText(description);

        cancelBtn.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                onBackPressed();
            }
        });

        editNote.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                Note note = new Note(editTitle.getText().toString(), editDescription.getText().toString());
                Intent intent = getIntent();
                note.setId(intent.getIntExtra("id",1));

                if (new NoteHandler(EditNote.this).update(note)) {
                    Toast.makeText(EditNote.this, "Note is updated!", Toast.LENGTH_SHORT).show();
                } else {
                    Toast.makeText(EditNote.this, "Note is not updated!", Toast.LENGTH_SHORT).show();
                }
                onBackPressed();
            }
        });


    }

}