package com.example.nicke.mojaevidencija.Fragment;

import android.app.Dialog;
import android.support.v4.app.DialogFragment;
import android.os.Bundle;
import android.view.LayoutInflater;
import android.view.View;

import com.example.nicke.mojaevidencija.R;
import com.example.nicke.mojaevidencija.Util.AlertDialogHelper;

/**
 * Created on 3/18/2018.
 */

public class CategoryEditTitleFragmentDialog extends DialogFragment {

    AlertDialogHelper.ActionBarListener actionBarListener;

    @Override
    public Dialog onCreateDialog(Bundle savedInstanceState) {
        View view = LayoutInflater.from(getActivity()).inflate(R.layout.dialog_category, null);
        return AlertDialogHelper.editCategoryTitle(view, actionBarListener);
    }

    public void setActionBarListener(AlertDialogHelper.ActionBarListener actionBarListener) {
        this.actionBarListener = actionBarListener;
    }
}