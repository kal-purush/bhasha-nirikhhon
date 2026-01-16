package com.android.varun.journalentry;

import android.content.Intent;
import android.database.Cursor;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.util.Log;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.ImageButton;
import android.widget.LinearLayout;
import android.widget.SimpleCursorAdapter;
import android.widget.TextView;

import com.android.varun.journalentry.data.MyDB;
import com.android.varun.journalentry.data.constants;

public class detailsActivity extends AppCompatActivity {
    TextView title, mood, details, date;
    int position;
    @Override
    protected void onCreate(Bundle savedInstanceState){
        super.onCreate(savedInstanceState);

        setContentView(R.layout.details_layout);
        title = (TextView) findViewById(R.id.Title);
        mood = (TextView) findViewById(R.id.Mood);
        details = (TextView) findViewById(R.id.Details);
        date = (TextView) findViewById(R.id.Date);

        position = getIntent().getIntExtra("clicked_item",0);
        SimpleCursorAdapter adapter = (SimpleCursorAdapter) MainActivity.entryList.getAdapter();
        Cursor myCursor = adapter.getCursor();

        myCursor.moveToPosition(position);

        title.setText(myCursor.getString(myCursor.getColumnIndexOrThrow(constants.TITLE_NAME)));
        mood.setText(myCursor.getString(myCursor.getColumnIndexOrThrow(constants.MOOD)));
        details.setText(myCursor.getString(myCursor.getColumnIndexOrThrow(constants.DETAIL_NAME)));
        date.setText(myCursor.getString(myCursor.getColumnIndexOrThrow(constants.DATE_NAME)));

    }

}