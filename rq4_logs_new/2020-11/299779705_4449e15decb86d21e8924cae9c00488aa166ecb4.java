package com.example.cmput301f20t18;

import android.app.AlertDialog;
import android.content.Context;
import android.content.Intent;
import android.os.Bundle;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.Button;

import androidx.annotation.NonNull;
import androidx.annotation.Nullable;

import com.google.android.material.bottomsheet.BottomSheetDialogFragment;

public class CustomBottomSheetDialog  extends BottomSheetDialogFragment {

    public static final int CANCEL_BUTTON = 0;
    public static final int EDIT_BUTTON = 1;
    public static final int DELETE_BUTTON = 2;

    private BottomSheetListener mListener;
    private boolean owner;
    private int status; //bookId

    public CustomBottomSheetDialog(boolean owner, int status) {
        this.owner = owner;
        this.status = status;
    }

    @Nullable
    @Override
    public View onCreateView(@NonNull LayoutInflater inflater, @Nullable ViewGroup container, @Nullable Bundle savedInstanceState) {

        View view;

        Button buttonCancel;
        Button buttonEdit;
        Button buttonDelete;

        if (status == Book.STATUS_ACCEPTED) {
            if (owner) {
                view = inflater.inflate(R.layout.bottom_sheet_mybooks_with_cancel, container, false);
            } else {
                // This is the only bottom sheet in the Borrowed section
                view = inflater.inflate(R.layout.bottom_sheet_borrowed, container, false);
            }
        } else { // User owns the book
            view = inflater.inflate(R.layout.bottom_sheet_mybooks_no_cancel, container, false);
        }

        try {
            buttonCancel = view.findViewById(R.id.button_cancel_pick_up);
            buttonCancel.setOnClickListener(new View.OnClickListener() {
                @Override
                public void onClick(View v) {
                    mListener.onButtonClick(CANCEL_BUTTON, status);
                    dismiss();
                }
            });
        } catch (Exception ignored) {}

        try {
            buttonEdit = view.findViewById(R.id.button_edit_book);
            buttonDelete = view.findViewById(R.id.button_delete_book);

            buttonEdit.setOnClickListener(new View.OnClickListener() {
                @Override
                public void onClick(View v) {
                    mListener.onButtonClick(EDIT_BUTTON, status);
                    dismiss();
                }
            });

            buttonDelete.setOnClickListener(new View.OnClickListener() {
                @Override
                public void onClick(View v) {
                    mListener.onButtonClick(DELETE_BUTTON, status);
                    dismiss();
                }
            });
        } catch (Exception ignored) {}

        return view;
    }

    @Override
    public void onViewCreated(@NonNull View view, @Nullable Bundle savedInstanceState) {
        super.onViewCreated(view, savedInstanceState);
    }

    public interface BottomSheetListener {
        void onButtonClick(int button, int status);
    }

    @Override
    public void onAttach(@NonNull Context context) {
        super.onAttach(context);

        try {
            mListener = (BottomSheetListener) context;
        } catch (ClassCastException e) {
            throw new ClassCastException(context.toString() + " must implement BottomSheetListener");
        }
    }
}