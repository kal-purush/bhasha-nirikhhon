package com.example.sahnon.androidlabs;

import android.app.AlertDialog;
import android.app.Dialog;
import android.content.DialogInterface;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.support.v7.widget.Toolbar;
import android.util.Log;
import android.view.LayoutInflater;
import android.view.Menu;
import android.view.MenuItem;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.Toast;
import android.support.design.widget.Snackbar;

public class TestToolbar extends AppCompatActivity {

    Snackbar buttonSnackbar;
    String currentMessage;
    String initialMessage;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_test_toolbar);

        Toolbar lab8_toolbar = (Toolbar)findViewById(R.id.lab8_toolbar);
        setSupportActionBar(  lab8_toolbar) ;

        currentMessage = null;
        initialMessage = "Message to show";

        Button snackbarButton = (Button) findViewById(R.id.snackbarButton);

        snackbarButton.setOnClickListener(new View.OnClickListener() {
            public void onClick(View v) {
                buttonSnackbar.make(findViewById(R.id.snackbarButton), initialMessage, Snackbar.LENGTH_SHORT).show();
            }
        });

    }

    @Override
    public boolean onCreateOptionsMenu (Menu m){
        getMenuInflater().inflate(R.menu.toolbar_menu, m );
        return true;
    }

    @Override
    public boolean onOptionsItemSelected(MenuItem item){
        switch(item.getItemId()){
            case R.id.action_one:
                Log.d("Toolbar", "Action 1 selected");
                if(currentMessage == null)
                    currentMessage = "You selected item 1.";
                Snackbar.make(findViewById(R.id.snackbarButton), currentMessage, Snackbar.LENGTH_SHORT).show();

                return true;

            case R.id.action_two:
                Log.d("Toolbar", "Action 2 selected");

                AlertDialog.Builder builder = new AlertDialog.Builder(TestToolbar.this);
                builder.setTitle("Do you want to go back?");
                // Add the buttons
                builder.setPositiveButton("Ok", new DialogInterface.OnClickListener() {
                    public void onClick(DialogInterface dialog, int id) {
                        // User clicked OK button
                        finish();
                    }
                });
                builder.setNegativeButton("Cancel", new DialogInterface.OnClickListener() {
                    public void onClick(DialogInterface dialog, int id) {
                        // User cancelled the dialog
                    }
                });
                // Create the AlertDialog
                AlertDialog dialog = builder.create();
                dialog.show();

                return true;

            case R.id.action_three:
                Log.d("Toolbar", "Action 3 selected");

                AlertDialog.Builder imageBuilder = new AlertDialog.Builder(TestToolbar.this);

                imageBuilder.setView(R.layout.toolbar_alert);


                imageBuilder.setPositiveButton("Ok", new DialogInterface.OnClickListener() {
                    public void onClick(DialogInterface dialog, int id) {
                        // User clicked OK button
                        Dialog getText = (Dialog) dialog;

                        EditText viewText = (EditText) getText.findViewById(R.id.edit_query);

                        currentMessage = viewText.getText().toString();
                        initialMessage = currentMessage;
                    }
                });
                imageBuilder.setNegativeButton("Cancel", new DialogInterface.OnClickListener() {
                    public void onClick(DialogInterface dialog, int id) {
                        // User cancelled the dialog
                    }
                });

                // Create the AlertDialog
                AlertDialog imageDialog = imageBuilder.create();
                imageDialog.show();

                return true;

            case R.id.aboutOption:
                Log.d("Toolbar", "About selected");
                Toast.makeText(getBaseContext(), "Version 1.0, by Sahnon Mahbub" , Toast.LENGTH_SHORT ).show();
                return true;

            default:
                return super.onOptionsItemSelected(item);
        }
    }
}