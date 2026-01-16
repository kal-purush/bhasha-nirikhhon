package com.example.jatinm.activitytracker;

import android.app.Dialog;
import android.app.DialogFragment;
import android.content.DialogInterface;
import android.os.Bundle;
import android.support.annotation.NonNull;
import android.support.v7.app.AlertDialog;
import android.text.TextUtils;
import android.widget.EditText;

/**
 * Dialog to add a new activity. The dialogue box would
 * ask the user to enter activity name.
 */
public class AddNewActivityDialogFragment extends DialogFragment {
    private StorageDatabaseHelper mDatabaseHelper;
    private RefreshActivityListener mRefreshListener;

    public void setDatabaseHelper(@NonNull StorageDatabaseHelper databaseHelper) {
        mDatabaseHelper = databaseHelper;
    }

    @Override
    public Dialog onCreateDialog(Bundle savedInstanceState) {
        AlertDialog.Builder dialogBuilder = new AlertDialog.Builder(getActivity());
        dialogBuilder.setMessage("New activity");
        dialogBuilder.setPositiveButton("ok", new DialogInterface.OnClickListener() {
            @Override
            public void onClick(DialogInterface dialog, int which) {
                EditText editText = (EditText) AddNewActivityDialogFragment.this.getDialog()
                        .findViewById(R.id.new_activity_name);
                String name = editText.getText().toString();
                if (mDatabaseHelper != null && !TextUtils.isEmpty(name)) {
                    mDatabaseHelper.addNewActivity(name);
                }
                mRefreshListener.onRefresh();
            }
        });
        dialogBuilder.setNegativeButton("cancel", new DialogInterface.OnClickListener() {
            @Override
            public void onClick(DialogInterface dialog, int which) {
                dialog.cancel();
            }
        });
        dialogBuilder.setView(getActivity().getLayoutInflater().inflate(
                R.layout.activity_new, null));
        return dialogBuilder.create();

    }

    public void setRefreshListener(RefreshActivityListener refreshListener) {
        mRefreshListener = refreshListener;
    }
}