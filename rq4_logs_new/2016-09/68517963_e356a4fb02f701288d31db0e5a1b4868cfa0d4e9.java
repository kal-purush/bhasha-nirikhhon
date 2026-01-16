package com.groomify.hollavirun;

import android.support.v4.content.ContextCompat;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.util.Log;
import android.view.View;
import android.widget.TextView;
import android.widget.Toast;

public class TeamSelectActivity extends AppCompatActivity {

    private final static String TAG =  TeamSelectActivity.class.getSimpleName();

    private static TextView teamSelectionOneTextView;
    private static TextView teamSelectionTwoTextView;
    private static TextView teamSelectionThreeTextView;

    private static TextView letsGoTextView;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_team_select);

        if(teamSelectionOneTextView == null){
            teamSelectionOneTextView = (TextView) findViewById(R.id.text_view_team_one);
        }

        if(teamSelectionTwoTextView == null){
            teamSelectionTwoTextView = (TextView) findViewById(R.id.text_view_team_two);
        }

        if(teamSelectionThreeTextView== null){
            teamSelectionThreeTextView = (TextView) findViewById(R.id.text_view_team_three);
        }

        if(letsGoTextView== null){
            letsGoTextView = (TextView) findViewById(R.id.text_view_lets_go);
        }

        teamSelectionOneTextView.setOnClickListener(new TextView.OnClickListener() {
            @Override
            public void onClick(View v) {
                toggleTeamSelection(1);
            }
        });

        teamSelectionTwoTextView.setOnClickListener(new TextView.OnClickListener() {
            @Override
            public void onClick(View v) {
                toggleTeamSelection(2);
            }
        });

        teamSelectionThreeTextView.setOnClickListener(new TextView.OnClickListener() {
            @Override
            public void onClick(View v) {
                toggleTeamSelection(3);
            }
        });

        letsGoTextView.setOnClickListener(new TextView.OnClickListener() {
            @Override
            public void onClick(View v) {
                Log.i(TAG, "Lets go clicked. Run run run.");
            }
        });

    }

    private void toggleTeamSelection(int currentTeam){
        teamSelectionOneTextView.setTextColor(ContextCompat.getColor(this,R.color.primaryTextColour));
        teamSelectionTwoTextView.setTextColor(ContextCompat.getColor(this,R.color.primaryTextColour));
        teamSelectionThreeTextView.setTextColor(ContextCompat.getColor(this,R.color.primaryTextColour));

        switch (currentTeam){
            case 1:
                teamSelectionOneTextView.setTextColor(ContextCompat.getColor(this,R.color.rustyRed));
                break;
            case 2:
                teamSelectionTwoTextView.setTextColor(ContextCompat.getColor(this,R.color.rustyRed));
                break;
            case 3:
                teamSelectionThreeTextView.setTextColor(ContextCompat.getColor(this,R.color.rustyRed));
                break;
            default:
                break;
        }

    }
}