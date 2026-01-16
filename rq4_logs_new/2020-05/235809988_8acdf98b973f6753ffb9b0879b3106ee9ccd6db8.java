package com.group4sweng.scranplan.Administration;

import android.app.Activity;
import android.app.AlertDialog;
import android.content.DialogInterface;
import android.view.LayoutInflater;
import android.view.View;
import android.widget.EditText;
import android.widget.ImageView;
import android.widget.Toast;

import com.google.firebase.firestore.CollectionReference;
import com.google.firebase.firestore.DocumentReference;
import com.google.firebase.firestore.FirebaseFirestore;
import com.group4sweng.scranplan.MainActivity;
import com.group4sweng.scranplan.R;

import java.util.HashMap;

import static androidx.test.InstrumentationRegistry.getContext;

public class ContentReporting {

    Activity activity;
    private HashMap<String, Object> document;
    private FirebaseFirestore mDatabase = FirebaseFirestore.getInstance();

    public ContentReporting(Activity thisActivity, HashMap<String, Object> map){
        activity = thisActivity;
        this.document = map;
    }

    public void startReportingDialog(){
        AlertDialog.Builder report = new AlertDialog.Builder(activity);

//        LayoutInflater inflater = activity.getLayoutInflater();
//        View view = inflater.inflate(null, null);
//        report.setView(view);

        final EditText edittext = new EditText(activity);
        report.setMessage("What is the issue you would like to report?");
        report.setTitle("Report Content");

        report.setView(edittext);

        report.setPositiveButton("Submit", new DialogInterface.OnClickListener() {
            public void onClick(DialogInterface dialog, int whichButton) {
                String usersContentReport = edittext.getText().toString();

                document.put("report", usersContentReport);

                CollectionReference reportRef = mDatabase.collection("reporting");
                DocumentReference documentReference = reportRef.document();
                documentReference.set(document);

                Toast.makeText(activity, "Thank you for your feedback",
                        Toast.LENGTH_SHORT).show();
            }
        });

        report.setNegativeButton("Cancel", new DialogInterface.OnClickListener() {
            public void onClick(DialogInterface dialog, int whichButton) {
                dialog.dismiss();
            }
        });

        report.show();

    }

}