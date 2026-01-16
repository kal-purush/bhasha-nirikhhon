package com.bradleyboxer.scavengerhunt;

import android.app.AlertDialog;
import android.content.DialogInterface;
import android.content.Intent;
import android.support.annotation.ArrayRes;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.util.Base64;
import android.util.Log;
import android.widget.EditText;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;

public class ScavengerHuntRecieverActivity extends AppCompatActivity {

    EditText enteredText;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_scavenger_hunt_reciever);

        enteredText = findViewById(R.id.serializedScavengerHunt);
    }

    public ArrayList<Clue> deserialize(String serializedObject) throws Exception {
        byte b[] = Base64.decode(serializedObject.getBytes(), Base64.DEFAULT);
        ByteArrayInputStream bi = new ByteArrayInputStream(b);
        ObjectInputStream si = new ObjectInputStream(bi);
        ArrayList<Clue> clueList = (ArrayList<Clue>) si.readObject();
        return clueList;
    }

    @Override
    public void onBackPressed() {
        try {
            Intent intent = new Intent();
            String text = enteredText.getText().toString().trim();
            ArrayList<Clue> clueList = deserialize(text);
            intent.putExtra("clueList", clueList);
            setResult(RESULT_OK, intent);
            finish();
        } catch (Exception e) {
            Log.e("GEOFENCE UI", "serialization error", e);

            AlertDialog.Builder builder = new AlertDialog.Builder(this);
            builder.setMessage("Incorrect object serialization data. Please correct and try again.");
            builder.setPositiveButton("Try Again", new DialogInterface.OnClickListener() {
                public void onClick(DialogInterface dialog, int id) {
                    //do nothing, they will try again
                }
            });
            builder.setNegativeButton("Cancel", new DialogInterface.OnClickListener() {
                public void onClick(DialogInterface dialog, int id) {
                    setResult(RESULT_CANCELED);
                    finish();
                }
            });

            AlertDialog dialog = builder.create();
            dialog.show();

        }
    }
}