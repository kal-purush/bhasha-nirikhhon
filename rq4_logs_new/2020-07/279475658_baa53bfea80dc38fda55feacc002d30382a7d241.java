package com.example.cst2335.deezer;

import android.content.Context;
import android.content.DialogInterface;

import androidx.appcompat.app.AlertDialog;

/**
 * Helps to show the AlertDialog
 * */
class DialogHelper {

    /**
     * @param context Context to build the alert dialog object
     * @param title to set as a title of an AlertDialog
     * @param message to set as a message of AlertDialog
     * @param positiveText to show a Label on the alert dialog button
     * */
    static void showHelpDialog(Context context,
                               String title,
                               String message,
                               String positiveText) {
        AlertDialog.Builder alertDialog = new AlertDialog.Builder(context);
        alertDialog.setTitle(title);
        alertDialog.setMessage(message);
        alertDialog.setPositiveButton(positiveText, new DialogInterface.OnClickListener() {
            @Override
            public void onClick(DialogInterface dialog, int which) {
                dialog.dismiss();
            }
        });
        alertDialog.show();
    }
}