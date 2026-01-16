package com.example.justpoteito.utilities;

import android.content.Context;
import android.widget.EditText;

import com.example.justpoteito.R;

public class FormValidator {

    private static Context context;

    public FormValidator (Context context) {
        this.context = context;
    }

    public static boolean editTextIsValid(EditText editText, int minimumLength, boolean isEmail) {
        String text = editText.getText().toString().trim();
        editText.setError(null);

        if (text.length() == 0 && !isEmail) {
            editText.setError(context.getString(R.string.empty_form_field));
            return false;
        }

        if (text.length() > 0 && text.length() < minimumLength && !isEmail) {
            editText.setError(context.getString(R.string.short_form_filed) + " " + minimumLength + " " + context.getString(R.string.character));
            return false;
        }

        if (isEmail && !android.util.Patterns.EMAIL_ADDRESS.matcher(text).matches()) {
            editText.setError(context.getString(R.string.email_required_form_field));
            return false;
        }

        return true;
    }
}