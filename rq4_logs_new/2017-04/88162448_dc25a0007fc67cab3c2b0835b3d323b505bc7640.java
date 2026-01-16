package br.pro.hashi.ensino.desagil.morse;

import android.app.AlertDialog;
import android.content.Context;
import android.content.DialogInterface;

/**
 * Created by jean-low on 17/04/17.
 */


public class Utilities {
    public static void confirm(final UtilityActivity activity){
        String label = "Are you sure?";

        AlertDialog.Builder builder = new AlertDialog.Builder(activity);

        builder.setCancelable(true);
        builder.setTitle("Title");
        builder.setMessage("Message");
        builder.setPositiveButton("Confirm",
                new DialogInterface.OnClickListener() {
                    @Override
                    public void onClick(DialogInterface dialog, int which) {
                        activity.listenConfirm(true);
                    }
                });
        builder.setNegativeButton(android.R.string.cancel, new DialogInterface.OnClickListener() {
            @Override
            public void onClick(DialogInterface dialog, int which) {
                activity.listenConfirm(false);
            }
        });

        AlertDialog dialog = builder.create();
        dialog.show();
    }
}