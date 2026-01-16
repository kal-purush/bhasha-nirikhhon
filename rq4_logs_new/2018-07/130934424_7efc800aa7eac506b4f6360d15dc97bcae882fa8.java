package com.bradleyboxer.scavengerhunt;

import android.app.AlertDialog;
import android.content.DialogInterface;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.util.AttributeSet;
import android.view.View;
import android.widget.Button;
import android.widget.LinearLayout;
import android.widget.ScrollView;

import java.util.ArrayList;
import java.util.List;

public class ClueCreatorActivity extends AppCompatActivity {

    LinearLayout scrollableClueSegments;

    List<ClueSegmentView> compassClueSegments;
    List<Button> deleteButtons;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_clue_creator);

        scrollableClueSegments = (LinearLayout) findViewById(R.id.scrollableClueSegments);
        deleteButtons = new ArrayList<>();
        compassClueSegments = new ArrayList<>();
    }

    public void onNewCompassClueSegment(View v) {
        ClueSegmentView clueSegmentView = new ClueSegmentView(this);
        Button deleteButton = new Button(this);

        deleteButton.setText(" ^ Delete Above Compass Segment ^ ");
        deleteButton.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                confirmRemoveDeleteButton(view);
            }
        });

        deleteButtons.add(deleteButton);
        compassClueSegments.add(clueSegmentView);

        scrollableClueSegments.addView(clueSegmentView);
        scrollableClueSegments.addView(deleteButton);
    }


    public void confirmRemoveDeleteButton(final View v) {
        AlertDialog.Builder builder = new AlertDialog.Builder(this);
        builder.setMessage("Are you sure you want to delete this clue segment?");
        builder.setPositiveButton("Yes, delete this segment!", new DialogInterface.OnClickListener() {
            public void onClick(DialogInterface dialog, int id) {
                removeDeleteButton(v);
            }
        });
        builder.setNegativeButton("No, wait, dont!", new DialogInterface.OnClickListener() {
            public void onClick(DialogInterface dialog, int id) {
                //user cancelled, do nothing
                // :(
            }
        });

        AlertDialog dialog = builder.create();
        dialog.show();
    }

    public void removeDeleteButton(View v) {
        for(int i=0;i<deleteButtons.size();i++) {
            Button button = deleteButtons.get(i);
            if(button.equals(v)) {
                scrollableClueSegments.removeView(button);
                scrollableClueSegments.removeView(compassClueSegments.get(i));

                deleteButtons.remove(i);
                compassClueSegments.remove(i);
            }
        }
    }
}