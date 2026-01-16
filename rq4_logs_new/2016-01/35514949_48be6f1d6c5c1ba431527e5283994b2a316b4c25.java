package com.appspot.simple_ticker.hartenholmticker.ui.ticker;

import android.os.Bundle;
import android.support.v4.app.DialogFragment;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.Button;
import android.widget.GridView;

import com.appspot.simple_ticker.hartenholmticker.R;

public class IconChooserDialog extends DialogFragment
{
    private static final String KEY_SELECTION = "KEY_ICON_SELECTION";

    private int _selection = -1;
    private GridView _gridView;

    public IconChooserDialog()
    {}

    @Override
    public View onCreateView(LayoutInflater inflater, ViewGroup container,
                             Bundle savedInstanceState) {

        if (savedInstanceState != null)
        {
            _selection = savedInstanceState.getInt(KEY_SELECTION);
        }

        View view = inflater.inflate(R.layout.fragment_icon_chooser, container);
        Button closeBtn = (Button) view.findViewById(R.id.icon_chooser_exit_btn);
        closeBtn.setOnClickListener(view2 -> {
            ((TickerEntryActivity)getActivity()).setIcon(_selection);
            dismiss();
        });

        _gridView = (GridView) view.findViewById(R.id.imageGrid);
        _gridView.setAdapter(new ImageAdapter(getContext()));
        _gridView.setOnItemClickListener((adapterView, view1, position, l) -> setSelection(position));

        getDialog().setTitle("Choose Icon");

        return view;
    }

    @Override
    public void onSaveInstanceState(Bundle outState)
    {
        super.onSaveInstanceState(outState);
        outState.putInt(KEY_SELECTION, _selection);
    }

    public void setSelection(int position)
    {
        System.out.println("selected " + position);

        _gridView.setItemChecked(position, _selection != position);
        if (_selection != -1)
        {
            _gridView.setItemChecked(_selection, false);
        }
        _selection = position;
    }
}