package com.edavtyan.flashlight;

import android.app.AlertDialog;
import android.app.Dialog;
import android.app.DialogFragment;
import android.content.DialogInterface;
import android.os.Bundle;
import android.support.annotation.NonNull;

public class NotSupportedDialogFragment
        extends DialogFragment
        implements DialogInterface.OnClickListener {
    @NonNull
    @Override
    public Dialog onCreateDialog(Bundle savedInstanceState) {
        AlertDialog.Builder builder = new AlertDialog.Builder(getActivity());
        builder.setTitle(R.string.NotSupportedDialog_Title);
        builder.setMessage(R.string.NotSupportedDialog_Message);
        builder.setPositiveButton(R.string.NotSupportedDialog_PositiveButton, this);
        return builder.create();
    }

    @Override
    public void onClick(DialogInterface dialog, int which) {
        System.exit(1);
    }
}