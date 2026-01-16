package com.example.tofunmi.thermalprinterproject;

import android.app.AlertDialog;
import android.app.Dialog;
import android.content.Context;
import android.content.DialogInterface;
import android.net.Uri;
import android.os.Bundle;
import android.support.v4.app.DialogFragment;
import android.support.v4.app.Fragment;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.view.inputmethod.InputMethodManager;
import android.widget.EditText;
import android.widget.LinearLayout;

import org.json.JSONException;
import org.json.JSONObject;


public class AddTerminalFragment extends DialogFragment {
    public int subterminalClicks = 0;

    public Dialog onCreateDialog(Bundle savedInstanceState) {

        LayoutInflater inflater = getActivity().getLayoutInflater();
        // Use the Builder class for convenient dialog construction

        AlertDialog.Builder builder = new AlertDialog.Builder(getActivity());

        final View view = inflater.inflate(R.layout.fragment_add_terminal, null);

        builder.setView(view);

        builder.setPositiveButton("Submit", new DialogInterface.OnClickListener() {
            public void onClick(DialogInterface dialog, int id) {
                // Generates the terminal json
                try {
                    JSONObject json = generateTerminalJSON(view);
                    new SpecifyTerminalTask((Loginable)getActivity()).execute(json.toString());
                }
                catch(JSONException e) {
                    System.out.println("There was an error");
                }

            }
        });

        builder.setNegativeButton("Cancel", new DialogInterface.OnClickListener() {
            public void onClick(DialogInterface dialog, int id) {
                // User cancelled the dialog
            }
        });

        // Adding onClicklistener to add new terminals
        view.findViewById(R.id.add_terminal_button).setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                LinearLayout layout = (LinearLayout) view.findViewById(R.id.subterminal_layout);
                LinearLayout subterminalView = (LinearLayout) getLayoutInflater().inflate(R.layout.subterminal_spec, null);
                EditText terminalNameField = (EditText)subterminalView.getChildAt(0);
//                // Set focus
//                setTextFocus(terminalNameField, (InputMethodManager) getContext().getSystemService(Context.INPUT_METHOD_SERVICE));

//                subterminalView.setLayoutParams(new LinearLayout.LayoutParams(0, LinearLayout.LayoutParams.MATCH_PARENT, 1));
                subterminalView.setId(subterminalClicks);
                layout.addView(subterminalView);
                subterminalClicks++;
            }
        });

        // Create the AlertDialog object and return it
        return builder.create();

    }

    // This calls generateSubterminal to get all additional added subterminals and
    private JSONObject generateTerminalJSON(View view) throws JSONException {
        JSONObject returnedObject = new JSONObject();
        String mainTerminalName = ((EditText)view.findViewById(R.id.main_terminal_name)).getText().toString();
        String mainTerminalPassword = ((EditText)view.findViewById(R.id.main_terminal_password)).getText().toString();
        JSONObject subterminals = generateSubterminalJSON(view, 0);
        returnedObject.put("terminalName", mainTerminalName);
        returnedObject.put("terminalPassword", mainTerminalPassword);
        returnedObject.put("subterminal", subterminals);
        return returnedObject;
    }

    // This gets all subterminals
    private JSONObject generateSubterminalJSON(View view, int i) throws JSONException {
        JSONObject returnedObject = new JSONObject();
        if (i >= subterminalClicks) {
            return returnedObject;
        }
        else {
            LinearLayout layout = (LinearLayout) view.findViewById(i);
            returnedObject.put("terminalName", ((EditText) layout.getChildAt(0)).getText());
            returnedObject.put("terminalPassword", ((EditText) layout.getChildAt(1)).getText());
            returnedObject.put("subterminal", generateSubterminalJSON(view, i+1));
            return returnedObject;
        }
    }


}