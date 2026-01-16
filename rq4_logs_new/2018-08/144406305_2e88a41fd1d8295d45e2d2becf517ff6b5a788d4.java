package com.cryptojobboard.cryptojobboard.feed;

import android.content.Intent;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.view.View;

import com.cryptojobboard.cryptojobboard.R;
import com.cryptojobboard.cryptojobboard.UserTypeSelection;

public class LandingPageActivity extends AppCompatActivity {

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_landing_page);
        setTitle("");
        getSupportActionBar().setElevation(0);

    }

    public void getStartedButton( View view )
    {
        Intent startIntent = new Intent(this.getApplicationContext(), UserTypeSelection.class);
        startActivity( startIntent );
    }
}