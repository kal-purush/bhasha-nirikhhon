package araragi.doctorsnotebook.activity;

import android.app.AlertDialog;
import android.content.DialogInterface;
import android.os.Bundle;
import android.support.v7.app.ActionBarActivity;
import android.view.View;
import android.widget.Toast;

import araragi.doctorsnotebook.R;
import araragi.doctorsnotebook.database.DBAdapter;

/**
 * Created by araragi on 27.04.16.
 */
public class DatabaseActivity extends ActionBarActivity{

    DBAdapter patientDataBase;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.database_activity);
        openDB();

    }
    @Override
    protected void onDestroy() {

        closeDB();
        super.onDestroy();

    }



    private void openDB() {
        patientDataBase = new DBAdapter(this);
        patientDataBase.open();

    }

    private void closeDB() {
        patientDataBase.close();
    }



    public void deleteAllAlertDialog(View v){
        AlertDialog.Builder builder = new AlertDialog.Builder(DatabaseActivity.this);
        builder.setTitle("WARNING!!!")
                .setMessage("Are you sure you want to destroy all data!?")
                .setCancelable(true)
                .setNegativeButton("Cancel", new DialogInterface.OnClickListener() {
                    @Override
                    public void onClick(DialogInterface dialog, int which) {
                        Toast.makeText(DatabaseActivity.this, "Good boy", Toast.LENGTH_LONG).show();
                    }
                }).
                setPositiveButton("DELETE", new DialogInterface.OnClickListener() {
                    @Override
                    public void onClick(DialogInterface dialog, int which) {
                        patientDataBase.deleteAll();
                        Toast.makeText(DatabaseActivity.this, "DELETED!!!", Toast.LENGTH_LONG).show();
                    }
                });

        AlertDialog alert = builder.create();
        alert.show();


    }
}