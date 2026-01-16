package com.apptec.camello.mainactivity.fpermission.ui;

import android.app.Dialog;
import android.os.Bundle;
import android.view.LayoutInflater;
import android.view.View;
import android.widget.TextView;

import androidx.annotation.NonNull;
import androidx.annotation.Nullable;
import androidx.appcompat.app.AlertDialog;
import androidx.fragment.app.DialogFragment;

import com.apptec.camello.R;

import timber.log.Timber;

public class ConfirmActionDialog extends DialogFragment {


    TextView title, message;

    int titleContent, messageContent;

    // Listener
    ResponseListener listener;


    /**
     * The response listener
     */
    public interface ResponseListener {

        /**
         * Method when the user confirm the action
         */
        void onAccepted();

        /**
         * Method if the user cancel the dialog
         */
        void onCanceled();
    }


    /**
     * Constructor to set the content of the title and the message
     *
     * @param titleContent   title of the dialog
     * @param messageContent message of the dialog
     */
    public ConfirmActionDialog(int titleContent, int messageContent, ResponseListener listener) {
        this.titleContent = titleContent;
        this.messageContent = messageContent;
        this.listener = listener;
    }

    /**
     * @param savedInstanceState The last saved instance state of the Fragment,
     *                           or null if this is a freshly created Fragment.
     * @return Return a new Dialog instance to be displayed by the Fragment.
     */
    @NonNull
    @Override
    public Dialog onCreateDialog(@Nullable Bundle savedInstanceState) {

        AlertDialog.Builder builder = new AlertDialog.Builder(getActivity(), R.style.DialogTheme);
        LayoutInflater inflater = requireActivity().getLayoutInflater();
        View viewDialog = inflater.inflate(R.layout.dialog_base, null);

        // Binding the UI elements
        title = viewDialog.findViewById(R.id.dialog_base_title);
        message = viewDialog.findViewById(R.id.dialog_base_message);


        // Setting the content for the elements (Be sure the method setNotification i called before this happens)
        title.setText(titleContent);
        message.setText(messageContent);

        // Set a ok button to close the dialog
        builder.setView(viewDialog)
                .setPositiveButton("ACEPTAR", (dialog, id) -> {
                    // Continue with the action
                    Timber.d("Ok clicked");
                    if (listener != null) {
                        listener.onAccepted();
                    }
                    ConfirmActionDialog.this.getDialog().cancel();
                })
                .setNegativeButton("CANCELAR", (dialog, which) -> {
                    Timber.d("Canceled");
                    if (listener != null) {
                        listener.onCanceled();
                    }
                    ConfirmActionDialog.this.getDialog().cancel();
                });

        return builder.create();
    }


}