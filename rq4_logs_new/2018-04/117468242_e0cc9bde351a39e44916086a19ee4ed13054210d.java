package com.thebipolaroptimist.projecttwo.dialogs;

import android.app.Dialog;
import android.content.Context;
import android.content.SharedPreferences;
import android.os.Build;
import android.os.Bundle;
import android.preference.PreferenceManager;
import android.support.annotation.NonNull;
import android.support.v4.app.DialogFragment;
import android.util.Log;

import com.thebipolaroptimist.projecttwo.R;
import com.thebipolaroptimist.projecttwo.SettingsFragment;
import com.thebipolaroptimist.projecttwo.models.DetailDTO;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public class BaseDetailDialog extends DialogFragment {
    private static final String TAG = "BaseDetailDialog: ";
    protected String mCategory = "";
    protected List<String> detailsTypes = new ArrayList<>();
    protected Map<String,String> detailTypesToColors = new HashMap<>();
    protected DetailDialogListener mListener;;

    public interface DetailDialogListener
    {
        void onDetailDialogPositiveClick(DetailDTO activityDetailDTO, String category);
        void onDetailDialogPositiveClick(List<DetailDTO> activityDetailDTO, String category);
    }

    @Override
    public void onAttach(Context context) {
        super.onAttach(context);

        try {
            mListener = (DetailDialogListener) context;
        } catch (ClassCastException e) {
            Log.e(TAG, "ClassCastException " + e.getMessage());
        }
    }

    @NonNull
    @Override
    public Dialog onCreateDialog(Bundle savedInstanceState) {
        SharedPreferences prefs = PreferenceManager.getDefaultSharedPreferences(getActivity());

        final List<String> activitiesFromPrefs = new ArrayList<>();
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.N) {
            activitiesFromPrefs.addAll(prefs.getStringSet(SettingsFragment.PREFERENCE_PREFIX + mCategory, new HashSet<String>()));
        } else
        {
            String activityString = prefs.getString(SettingsFragment.PREFERENCE_PREFIX + mCategory, "");
            activitiesFromPrefs.addAll(Arrays.asList(activityString.split(",")));
        }


        detailsTypes.add(getString(R.string.dialog_activity_spinner_label));
        for (String preference : activitiesFromPrefs) {
            String[] pieces = preference.split(":");
            if(pieces.length >1) {
                detailsTypes.add(pieces[0]);
                detailTypesToColors.put(pieces[0], pieces[1]);
            } else
            {
                Log.w(TAG + mCategory, "Invalid activity type stored");
            }
        }
        return null;
    }
}