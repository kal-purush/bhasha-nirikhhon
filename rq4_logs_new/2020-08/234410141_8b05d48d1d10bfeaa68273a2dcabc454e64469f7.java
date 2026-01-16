package com.apptec.camello.mainactivity;

import android.app.ProgressDialog;
import android.content.Context;
import android.widget.TextView;

import com.apptec.camello.R;

import timber.log.Timber;

public class CustomProgressDialog extends ProgressDialog {

    private TextView mMessageView;

    /**
     * Creates a Progress dialog.
     *
     * @param context the parent context
     */
    public CustomProgressDialog(Context context) {
        super(context);
    }

    /**
     * Creates a Progress dialog.
     *
     * @param context the parent context
     * @param theme   the resource ID of the theme against which to inflate
     *                this dialog, or {@code 0} to use the parent
     *                {@code context}'s default alert dialog theme
     */
    public CustomProgressDialog(Context context, int theme) {
        super(context, theme);
    }

    @Override
    public void setMessage(CharSequence message) {
        mMessageView = (TextView) findViewById(R.id.dialog_success_message);
        try {
            mMessageView.setText(message);
        } catch (Exception e) {
            Timber.w(e);
        }
    }
}