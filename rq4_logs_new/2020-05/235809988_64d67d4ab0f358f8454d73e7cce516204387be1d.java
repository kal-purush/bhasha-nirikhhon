package com.group4sweng.scranplan.RecipeCreation;

import android.app.Activity;
import android.content.DialogInterface;
import android.content.Intent;
import android.graphics.Color;
import android.os.Bundle;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.Button;
import android.widget.Spinner;
import android.widget.Toast;

import androidx.appcompat.app.AppCompatDialogFragment;

import com.flask.colorpicker.ColorPickerView;
import com.flask.colorpicker.builder.ColorPickerClickListener;
import com.flask.colorpicker.builder.ColorPickerDialogBuilder;
import com.group4sweng.scranplan.R;

public class RecipeDefaultsCreation extends AppCompatDialogFragment {
    private Button mBackgroundButton;
    private Spinner mSlideFont;
    private Spinner mFontSize;
    private Button mFontColourButton;
    private Button mSubmit;

    private Integer mBackgroundColour;
    private String mFont;
    private String mSize;
    private Integer mFontColour;

    @Override
    public void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
    }

    @Override
    public View onCreateView(LayoutInflater inflater, ViewGroup container,
                             Bundle savedInstanceState) {
        getDialog().setTitle("Slide settings");

        View layout = inflater.inflate(R.layout.create_recipe_dialog, container, false);

        initBundleItems(getArguments());
        initPageItems(layout);
        initPageListeners(layout);

        return layout;
    }

    private void initBundleItems(Bundle bundle) {
        mBackgroundColour = bundle.getInt("backColour");
        mFont = bundle.getString("font");
        mSize = bundle.getString("size");
        mFontColour = bundle.getInt("fontColour");
    }

    private void initPageItems(View view) {
        mBackgroundButton = view.findViewById(R.id.slideBackgroundPicker);
        mBackgroundButton.setBackgroundColor(mBackgroundColour);

        mSlideFont = view.findViewById(R.id.slideDefaultsFontSelect);
        mFontSize = view.findViewById(R.id.slideDefaultsFontSize);

        mFontColourButton = view.findViewById(R.id.fontColourPicker);
        mFontColourButton.setBackgroundColor(mFontColour);

        mSubmit = view.findViewById(R.id.slideDefaultsSubmit);
    }

    private void initPageListeners(View view) {
        mBackgroundButton.setOnClickListener(v -> ColorPickerDialogBuilder
            .with(getContext())
            .setTitle("Choose color")
            .initialColor(Color.WHITE)
            .wheelType(ColorPickerView.WHEEL_TYPE.FLOWER)
            .density(12)
            .setOnColorSelectedListener(selectedColor -> Toast.makeText(getContext(),
                    "onColorSelected: 0x" + Integer.toHexString(selectedColor),
                    Toast.LENGTH_SHORT).show())
            .setPositiveButton("ok", (dialog, selectedColor, allColors) -> {
                mBackgroundButton.setBackgroundColor(selectedColor);
                mBackgroundColour = selectedColor; })
            .setNegativeButton("cancel", (dialog, which) -> {})
            .build()
            .show()
        );

        mFontColourButton.setOnClickListener(v -> ColorPickerDialogBuilder
            .with(getContext())
            .setTitle("Choose color")
            .initialColor(Color.WHITE)
            .wheelType(ColorPickerView.WHEEL_TYPE.FLOWER)
            .density(12)
            .setOnColorSelectedListener(selectedColor -> Toast.makeText(getContext(),
                    "onColorSelected: 0x" + Integer.toHexString(selectedColor),
                    Toast.LENGTH_SHORT).show())
            .setPositiveButton("ok", (dialog, selectedColor, allColors) -> {
                mFontColourButton.setBackgroundColor(selectedColor);
                mFontColour = selectedColor; })
            .setNegativeButton("cancel", (dialog, which) -> {})
            .build()
            .show()
        );

        mSubmit.setOnClickListener(v -> {
            Intent intent = new Intent();
            intent.putExtra("backColour", mBackgroundColour);
            intent.putExtra("font", mFont);
            intent.putExtra("size", mSize);
            intent.putExtra("fontColour", mFontColour);

            getTargetFragment().onActivityResult(getTargetRequestCode(), Activity.RESULT_OK, intent);
            dismiss();
        });
    }

}