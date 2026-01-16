package com.example.michaellongcrawford.week05;

import android.content.Intent;
import android.os.Bundle;
import android.support.design.widget.FloatingActionButton;
import android.support.design.widget.Snackbar;
import android.support.v7.app.AppCompatActivity;
import android.support.v7.widget.Toolbar;
import android.view.View;
import android.view.Menu;
import android.view.MenuItem;
import android.widget.EditText;

public class ActivityOne extends AppCompatActivity {

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_one);
        Toolbar toolbar = (Toolbar) findViewById(R.id.toolbar);
        setSupportActionBar(toolbar);

        FloatingActionButton fab = (FloatingActionButton) findViewById(R.id.fab);
        fab.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                Snackbar.make(view, "Replace with your own action", Snackbar.LENGTH_LONG)
                        .setAction("Action", null).show();
            }
        });
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.menu_activity_one, menu);
        return true;
    }

    @Override
    public boolean onOptionsItemSelected(MenuItem item) {
        // Handle action bar item clicks here. The action bar will
        // automatically handle clicks on the Home/Up button, so long
        // as you specify a parent activity in AndroidManifest.xml.
        int id = item.getItemId();

        //noinspection SimplifiableIfStatement
        if (id == R.id.action_settings) {
            return true;
        }

        return super.onOptionsItemSelected(item);
    }

    /** Called when the user clicks the Share button */
    public void outputMessage(View view) {
        // Do something in response to button
        Intent myIntent;
        myIntent = new Intent(ActivityOne.this, ActivityTwo.class);

        EditText text = (EditText)findViewById(R.id.bookText);
        String book = text.getText().toString();

        EditText textB = (EditText)findViewById(R.id.chapterText);
        String chapter = textB.getText().toString();

        EditText textC = (EditText)findViewById(R.id.verseText);
        String verse = textC.getText().toString();

        myIntent.putExtra("bookText", book ); //Optional parameters
        myIntent.putExtra("chapterText", chapter);
        myIntent.putExtra("verseText", verse);

        ActivityOne.this.startActivity(myIntent);

    }
}