package com.example.goldav.buybye;


import android.app.AlertDialog;
import android.app.Dialog;
import android.app.DialogFragment;
import android.content.DialogInterface;
import android.os.Bundle;
import android.view.Gravity;
import android.webkit.WebSettings;
import android.widget.TextView;

/**
 * Created by Aviv Gold on 6/13/2017.
 */

public class MyAlert extends DialogFragment {
    public String Message;


    @Override
    public Dialog onCreateDialog(Bundle savedInstanceState) {
        // Use the Builder class for convenient dialog construction
        AlertDialog.Builder builder = new AlertDialog.Builder(getActivity());
        TextView message = new TextView(getActivity());
        message.setText(Message);
        message.setTextSize(20);
        message.setGravity(Gravity.CENTER_HORIZONTAL);
        builder.setView(message)
                .setPositiveButton("ok", new DialogInterface.OnClickListener() {
                    public void onClick(DialogInterface dialog, int id) {
                        // FIRE ZE MISSILES!
                    }
                });


        return builder.create();
    }
}