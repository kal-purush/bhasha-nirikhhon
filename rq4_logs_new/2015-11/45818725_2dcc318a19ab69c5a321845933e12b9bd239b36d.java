package com.example.laurenfalzarano.shots;

import android.content.SharedPreferences;
import android.preference.PreferenceManager;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.util.Log;
import android.view.Menu;
import android.view.MenuItem;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.Toast;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class AddCounterActivity extends AppCompatActivity {

    public PreferencesManager manager;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_add_counter);

        final SharedPreferences app_preferences =
                PreferenceManager.getDefaultSharedPreferences(this);

        final PreferencesManager preferences = PreferencesManager.get();

        Button button = (Button) findViewById(R.id.submit_counter);
        final EditText nameField = (EditText) findViewById(R.id.counter_name);
        button.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                String name = nameField.getText().toString();
                System.out.println(name);

                // Get number of existing shots for this name
                int count = preferences.getShots(name);
                Log.d(name, Integer.toString(count));

                // Increment the counter
                preferences.incrementShots(name);
                nameField.setText("");

//                //Retrieve the values
//                Set<String> set = app_preferences.getStringSet("shotsList", null);
//                Set<String> newSet = new HashSet<>();
//
//                if (set != null) {
//                    // Do deep copy
//                    Iterator iter = set.iterator();
//                    while (iter.hasNext()) {
//                        newSet.add(iter.next().toString());
//                    }
//                }

                //Set the values
//
//                newSet.add(name);
//                editor.putStringSet("shotsList", newSet);
//                editor.commit();
//                System.out.println(app_preferences.getStringSet("shotsList", null));

                if (count > 1) {
                    Toast.makeText(getApplicationContext(), "The shots counter for " + name + " has been incremented.", Toast.LENGTH_LONG).show();
                } else {
                    Toast.makeText(getApplicationContext(), "A new shots counter for " + name + " has been created", Toast.LENGTH_LONG).show();
                }
            }
        });

    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.menu_add_counter, menu);
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


}