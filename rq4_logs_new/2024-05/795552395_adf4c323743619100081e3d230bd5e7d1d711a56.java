package com.hyep.movietracker.screens.fragment;

import android.content.Context;
import android.os.Bundle;

import androidx.annotation.NonNull;
import androidx.fragment.app.Fragment;

import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.Button;
import android.widget.Toast;

import com.google.android.material.bottomsheet.BottomSheetDialogFragment;
import com.hyep.movietracker.R;

public class BottomSheetSettingItemInSpaceFragment extends BottomSheetDialogFragment {


    private Button btnMoveTo, btnDeleteItem;

    private Context cont = getContext();

    public BottomSheetSettingItemInSpaceFragment() {
        // Required empty public constructor
    }

    @Override
    public void onAttach(@NonNull Context context) {
        super.onAttach(context);
        cont = context;
    }

    @Override
    public View onCreateView(LayoutInflater inflater, ViewGroup container,
                             Bundle savedInstanceState) {
        // Inflate the layout for this fragment
        View view = inflater.inflate(R.layout.fragment_bottom_sheet_setting_item_in_space, container, false);

        btnMoveTo = view.findViewById(R.id.btnMoveTo);
        btnDeleteItem = view.findViewById(R.id.btnDeleteItem);

        btnMoveTo.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                Toast.makeText(cont, "Move to", Toast.LENGTH_SHORT).show();
            }
        });
        btnDeleteItem.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                Toast.makeText(cont, "Delete item", Toast.LENGTH_SHORT).show();
            }
        });

        return view;
    }
}