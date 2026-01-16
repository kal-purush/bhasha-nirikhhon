package com.giovas.dialogs;

import android.app.Dialog;
import android.content.Context;
import android.text.Editable;
import android.text.TextWatcher;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;

import com.giovas.listeners.ZipCodeListener;
import com.giovas.weatherapp.R;

/**
 * Created by DarkGeat on 10/29/2015.
 */
public class AddZipCodeDialog extends Dialog {

    private Button add,cancel;
    private EditText zipCodeEntry;
    private ZipCodeListener listener;

    public AddZipCodeDialog(Context context) {
        super(context);
        setContentView(R.layout.zip_dialog);
        setTitle(context.getString(R.string.action_zip_codes).toUpperCase());
        add = (Button) findViewById(R.id.buttonAdd);
        cancel = (Button) findViewById(R.id.buttonCancel);
        add.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                listener.AddZipCode(zipCodeEntry.getText().toString());
                dismiss();
            }
        });
        cancel.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                dismiss();
            }
        });
        zipCodeEntry = (EditText)findViewById(R.id.zipcodeEntry);
        zipCodeEntry.addTextChangedListener(new TextWatcher() {
            @Override
            public void beforeTextChanged(CharSequence s, int start, int count, int after) {

            }

            @Override
            public void onTextChanged(CharSequence s, int start, int before, int count) {

            }

            @Override
            public void afterTextChanged(Editable s) {
                add.setEnabled(s.length() >= 5);
            }
        });
    }

    public void setListener(ZipCodeListener listener) {
        this.listener = listener;
    }
}