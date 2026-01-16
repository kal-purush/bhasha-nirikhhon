package com.simbiosyscorp.thetravelingsalesman;

import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.view.View;
import android.widget.Button;

import butterknife.Bind;
import butterknife.ButterKnife;

public class HelpActivity extends AppCompatActivity {

    @Bind(R.id.help_next)
    Button mNext;
    @Bind(R.id.help_0) View mFirst;
    int cnt = 0;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_help);

        ButterKnife.bind(this);


        mNext.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                mFirst.setVisibility(View.GONE);
            }
        });
    }
}