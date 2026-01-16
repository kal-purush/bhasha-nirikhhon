package fr.geringan.activdash.dialogs;

import android.app.Dialog;
import android.os.Bundle;
import android.support.annotation.NonNull;
import android.support.annotation.Nullable;
import android.support.v4.app.DialogFragment;
import android.support.v7.app.AlertDialog;
import android.widget.ArrayAdapter;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import fr.geringan.activdash.R;

public class ModeDialogFragment extends DialogFragment {

    private JSONArray modes;
    private SelectionListener selectionListener;

    public static ModeDialogFragment newInstance(String modes) {
        ModeDialogFragment fragment = new ModeDialogFragment();
        Bundle args = new Bundle();
        args.putString("modes", modes);
        fragment.setArguments(args);
        return fragment;
    }

    @Override
    public void onCreate(@Nullable Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
    }

    @NonNull
    @Override
    public Dialog onCreateDialog(@Nullable Bundle savedInstanceState) {

        AlertDialog.Builder builderSingle = new AlertDialog.Builder(getActivity());
        builderSingle.setIcon(R.drawable.ic_planning);
        builderSingle.setTitle(getString(R.string.select_a_mode));
        builderSingle.setNegativeButton("cancel", (dialog, which) -> dialog.dismiss());

        Bundle args = getArguments();
        if (null == args) {
            return builderSingle.create();
        }

        try {
            String modesStr = args.getString("modes");
            modes = new JSONArray(modesStr);

            final ArrayAdapter<String> arrayAdapter = new ArrayAdapter<>(getActivity(), android.R.layout.select_dialog_singlechoice);
            for (int i = 0; i < modes.length(); i++) {
                JSONObject obj = modes.getJSONObject(i);
                if (obj.has("nom")) {
                    arrayAdapter.add(obj.getString("nom"));
                }
            }
            builderSingle.setAdapter(arrayAdapter, (dialog, which) -> {
                String name = arrayAdapter.getItem(which);
                if (null == selectionListener) return;
                try {
                    jsonFromName(name);
                } catch (JSONException e) {
                    e.printStackTrace();
                }
            });

        } catch (JSONException e) {
            e.printStackTrace();
        }

        return builderSingle.create();
    }

    public void jsonFromName(String name) throws JSONException {
        for (int i = 0; i < modes.length(); i++) {
            JSONObject obj = modes.getJSONObject(i);
            if (obj.has("nom") && name.equals(obj.getString("nom"))) {
                try {
                    selectionListener.onSelectedItem(obj);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                return;
            }
        }
    }

    public void setSelectionListener(SelectionListener listener) {
        this.selectionListener = listener;
    }

    public interface SelectionListener {
        void onSelectedItem(JSONObject modes) throws JSONException, InterruptedException;
    }
}
