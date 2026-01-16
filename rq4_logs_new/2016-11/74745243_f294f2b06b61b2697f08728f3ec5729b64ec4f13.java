package de.fhdw.bfws115a.team1.caloriecounter.activities.communicationtestreceive;

import android.content.Intent;
import android.widget.TextView;
import de.fhdw.bfws115a.team1.caloriecounter.R;
import de.fhdw.bfws115a.team1.caloriecounter.entities.Unit;

public class ApplicationLogic {

    private Data mData;
    private Gui mGui;

    public ApplicationLogic(Data data, Gui gui) {
        mData = data;
        mGui = gui;
        initGui();
        initListener();
        doSmth();
    }

    private void initGui() {

    }

    private void initListener() {

    }

    private void doSmth() {
        Unit unit = (Unit) mData.getActivity().getIntent().getSerializableExtra("unit");
        //mGui.getDummyTextView().setText("test");
        mGui.getDummyTextView().setText(unit.getName());
    }
}