package jp.co.pitta.sensorlist;

import android.app.Activity;
import android.app.AlertDialog;
import android.app.Dialog;
import android.app.DialogFragment;
import android.content.DialogInterface;
import android.content.res.Resources;
import android.os.Bundle;
import android.support.v4.app.FragmentTransaction;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by shingo on 2016/06/25.
 */
public class FragmentSamplingTimeDialog extends DialogFragment {
    public static final String DATA = "items";

    public static final String SELECTED = "selected";

    private FragmentSensorDialogListener mListener;
    private int mItem;
    private Activity mActivity;

    @Override
    public Dialog onCreateDialog(Bundle savedInstanceState) {
        Resources res = getActivity().getResources();
        Bundle bundle = getArguments();
        mListener = (FragmentSensorDialogListener) getActivity();

        AlertDialog.Builder dialog = new AlertDialog.Builder(getActivity());

        dialog.setTitle("Select sampling time");
        dialog.setPositiveButton("OK", new PositiveButtonClickListener());
        dialog.setNegativeButton("Cancel", new NegativeButtonClickListener());

        List<String> list = getItems();
        int position = bundle.getInt(SELECTED);
        mItem = position;

        CharSequence[] cs = list.toArray(new CharSequence[list.size()]);
        dialog.setSingleChoiceItems(cs, position, selectItemListener);
        mActivity = getActivity();
        return dialog.create();
    }

    private ArrayList<String> getItems() {
        ArrayList<String> ret_val = new ArrayList<String>();

        ret_val.add("Normal");
        ret_val.add("UI");
        ret_val.add("Game");
        ret_val.add("Fastest");

        return ret_val;
    }

    class NegativeButtonClickListener implements DialogInterface.OnClickListener {
        @Override
        public void onClick(DialogInterface dialog, int which) {
            dialog.dismiss();
        }
    }

    class PositiveButtonClickListener implements DialogInterface.OnClickListener {
        @Override
        public void onClick(DialogInterface dialog, int which) {
            mListener.onFragmentSamplingTimeDialog(mItem);
            dialog.dismiss();
        }
    }

    DialogInterface.OnClickListener selectItemListener = new DialogInterface.OnClickListener() {

        @Override
        public void onClick(DialogInterface dialog, int which) {
            mItem = which;
            // process
            //which means position
            // dialog.dismiss();
        }

    };
}