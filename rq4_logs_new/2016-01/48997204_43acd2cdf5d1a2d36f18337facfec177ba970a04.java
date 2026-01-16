package no.bacheloroppgaven.android.learningbasics1;

import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.widget.TextView;

public class NotesActivity extends AppCompatActivity {

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_notes);

        initTextFields();
    }

    private void initTextFields() {
        TextView heading = (TextView) findViewById(R.id.txt_notes_heading);

        heading.setText("Notes");
    }
}