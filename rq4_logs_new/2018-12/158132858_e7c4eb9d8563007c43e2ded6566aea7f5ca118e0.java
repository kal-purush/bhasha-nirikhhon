package com.example.android.whatevertrash;

import android.content.Intent;
import android.os.Environment;
import android.support.constraint.ConstraintLayout;
import android.support.design.widget.FloatingActionButton;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.support.v7.widget.CardView;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class notetaking extends AppCompatActivity {

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.noteslayout);

        final source[] temp = (source[]) getIntent().getSerializableExtra("stream");
        final FloatingActionButton actionButton = findViewById(R.id.addnote);
        final EditText editUserNote = findViewById(R.id.editUserNote);
        final TextView viewUserNote = findViewById(R.id.viewUserNote);
        final ConstraintLayout notelayout = findViewById(R.id.Notelayout);
        final TextView locationName = findViewById(R.id.locationName);



        locationName.setText(temp[0].title);


        //This list will store user notes.
        //Auto fills in case of empty notes file.
        final ArrayList<String> userNotes = new ArrayList<String>();
        for (int i = 0; i < temp.length; i++) {
            userNotes.add("Enter your own notes here!");
        }

        //This will be the notes file.
        File dataDir = Environment.getDataDirectory();
        final File notesFile = new File(dataDir,"notes.txt");

        //This will try to open the notes file and add each line to a list entry
        try {
            BufferedReader br = new BufferedReader(new FileReader(notesFile));
            String line;
            int index = 0;
            while ((line = br.readLine()) != null) {
                userNotes.set(index, line);
                index++;
            }
            br.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }


        //This maps all the titles to integers.  I believe this is needed because of how
        //newsStream is altered.
        final Map<String, Integer> listMap = new HashMap<>();
        for (int j = 0; j < 35; j++ ) {
            listMap.put(temp[j].title, j);
        }

        editUserNote.setText(userNotes.get(listMap.get(temp[0].title)));

        notelayout.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                viewUserNote.setVisibility(View.GONE);
                editUserNote.setVisibility(View.VISIBLE);
            }
        });

        actionButton.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                try {
                    String toSave = editUserNote.getText().toString();
                    if (!toSave.trim().equals("")) {
                        try {
                            userNotes.set(listMap.get(temp[0].title), toSave);
                        } catch (NullPointerException e) {
                            e.printStackTrace();
                            throw e;
                        }
                        FileWriter fileWriter = new FileWriter(notesFile);
                        BufferedWriter writeOut = new BufferedWriter(fileWriter);
                        for (int i = 0; i < temp.length; i++) {
                            writeOut.write(userNotes.get(i));
                        }
                        writeOut.close();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
                viewUserNote.setVisibility(View.VISIBLE);
                editUserNote.setVisibility(View.GONE);
                try {
                    viewUserNote.setText(userNotes.get(listMap.get(temp[0].title)));
                } catch (NullPointerException e) {
                    e.printStackTrace();
                    throw e;
                }
            }
        });

        //This button will show the user notes for whichever element is in the 0 position
        //in the newsStream array.
        /*
        showUserNote.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                //show text box with note with a button underneath that when clicked will
                //make the note editable and change the button to a save note button.
                //will add a close view button too
                //Can we add something here that makes the background dark?  Maybe just make this
                //take up the whole screen?
                viewUserNote.setVisibility(View.VISIBLE);
                editUserNote.setVisibility(View.INVISIBLE);
                saveNoteButton.setVisibility(View.INVISIBLE);
                editNoteButton.setVisibility(View.VISIBLE);
                closeNoteButton.setVisibility(View.VISIBLE);
                showUserNote.setVisibility(View.INVISIBLE);
                try {
                    viewUserNote.setText(userNotes.get(listMap.get(temp[0].title)));
                } catch (NullPointerException e) {
                    e.printStackTrace();
                    throw e;
                }
            }
        });

        //This button will open up an edit text box for the user to input into.
        editNoteButton.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                try {
                    editUserNote.setText(userNotes.get(listMap.get(temp[0].title)));
                } catch (NullPointerException e) {
                    e.printStackTrace();
                    throw e;
                }
                viewUserNote.setVisibility(View.INVISIBLE);
                editUserNote.setVisibility(View.VISIBLE);
                saveNoteButton.setVisibility(View.VISIBLE);
                editNoteButton.setVisibility(View.INVISIBLE);
                closeNoteButton.setVisibility(View.VISIBLE);
                showUserNote.setVisibility(View.INVISIBLE);
            }
        });

        //This button will save the note to a text file.
        //Need to set path name.
        //Probably should change this to just use a single file and go line by line.
        saveNoteButton.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                try {
                    String toSave = editUserNote.getText().toString();
                    if (!toSave.trim().equals("")) {
                        try {
                            userNotes.set(listMap.get(temp[0].title), toSave);
                        } catch (NullPointerException e) {
                            e.printStackTrace();
                            throw e;
                        }
                        FileWriter fileWriter = new FileWriter(notesFile);
                        BufferedWriter writeOut = new BufferedWriter(fileWriter);
                        for (int i = 0; i < temp.length; i++) {
                            writeOut.write(userNotes.get(i));
                        }
                        writeOut.close();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
                viewUserNote.setVisibility(View.VISIBLE);
                editUserNote.setVisibility(View.INVISIBLE);
                saveNoteButton.setVisibility(View.INVISIBLE);
                editNoteButton.setVisibility(View.VISIBLE);
                closeNoteButton.setVisibility(View.VISIBLE);
                showUserNote.setVisibility(View.INVISIBLE);
                try {
                    viewUserNote.setText(userNotes.get(listMap.get(temp[0].title)));
                } catch (NullPointerException e) {
                    e.printStackTrace();
                    throw e;
                }
            }

        });

        //This is the close note button, should just close everything out.  Should not save anything.
        closeNoteButton.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                viewUserNote.setVisibility(View.INVISIBLE);
                editUserNote.setVisibility(View.INVISIBLE);
                saveNoteButton.setVisibility(View.INVISIBLE);
                editNoteButton.setVisibility(View.INVISIBLE);
                closeNoteButton.setVisibility(View.INVISIBLE);
                showUserNote.setVisibility(View.VISIBLE);
                viewUserNote.setText("");
                editUserNote.setText("");
            }
        });
        */
    }
}