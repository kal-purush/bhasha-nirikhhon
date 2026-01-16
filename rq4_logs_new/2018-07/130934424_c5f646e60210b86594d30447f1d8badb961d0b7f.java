package com.bradleyboxer.scavengerhunt;

import android.content.Context;
import android.util.AttributeSet;
import android.widget.Button;
import android.widget.EditText;
import android.widget.LinearLayout;
import android.widget.TextView;

import java.util.List;

public class ClueView extends LinearLayout {

    TextView textView;
    TextView textView2;
    Button upButton;
    Button downButton;
    Button editButton;
    Button deleteButton;
    Clue clue;

    public ClueView(Context context, Clue clue) {
        super(context, null, 0);
        init();
        if(clue!=null) {
            setClue(clue);
        }
    }

    public ClueView(Context context) {
        super(context, null, 0);
        init();
    }

    public ClueView(Context context, AttributeSet attrs) {
        super(context, attrs, 0);
        init();
    }

    public ClueView(Context context, AttributeSet attrs, int defStyle) {
        super(context, attrs, defStyle);
        init();
    }

    private void init() {
        setOrientation(VERTICAL);
        inflate(getContext(), R.layout.view_clue_creator, this);

        textView = findViewById(R.id.clueSummaryView);
        textView2 = findViewById(R.id.clueSummaryView2);
        upButton = findViewById(R.id.clueUpButton);
        downButton = findViewById(R.id.clueDownButton);
        editButton = findViewById(R.id.clueEditButton);
        deleteButton = findViewById(R.id.clueRemoveButton);
    }

    public void updateText() {
        textView.setText(""+clue.getGeofenceGeofenceData().getName()+"\n"+
        "("+clue.getGeofenceGeofenceData().latitude+", "+clue.getGeofenceGeofenceData().longitude+")");
        textView2.setText("... and "+(clue.getNumberOfSegments()-1)+" chained compass clue(s)");
    }

    public void setClue(Clue clue) {
        this.clue = clue;
        updateText();
    }

    public Clue getClue() {
        return clue;
    }
}