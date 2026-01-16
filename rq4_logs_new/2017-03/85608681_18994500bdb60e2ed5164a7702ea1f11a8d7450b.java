package com.helmetsmart.app.activity;

import android.content.Context;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.view.View;
import android.widget.ImageView;
import android.widget.TextView;

import com.helmetsmart.app.R;
import com.mikhaellopez.circularprogressbar.CircularProgressBar;

import butterknife.BindView;
import butterknife.ButterKnife;
import uk.co.chrisjenx.calligraphy.CalligraphyContextWrapper;

public class MyTripsActivity extends AppCompatActivity implements View.OnClickListener {

    @BindView(R.id.txtToolbarTitle)
    TextView txtToolbarTitle;
    @BindView(R.id.imgMenu)
    ImageView imgMenu;
    @BindView(R.id.progressHelmet)
    CircularProgressBar progressHelmet;
    @BindView(R.id.progressSpeed)
    CircularProgressBar progressSpeed;
    @BindView(R.id.progressDistance)
    CircularProgressBar progressDistance;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_my_trips);
        init();
    }

    /**
     * Method to init Data and Views
     */
    private void init() {
        ButterKnife.bind(this);
        txtToolbarTitle.setText(getString(R.string.str_my_trips));

        int animationDuration = 2500; // 2500ms = 2,5s
        progressHelmet.setProgressWithAnimation(40, animationDuration);
        progressSpeed.setProgressWithAnimation(60, animationDuration);
        progressDistance.setProgressWithAnimation(75, animationDuration);

        //Handling Click Listener for Views
        imgMenu.setOnClickListener(this);
    }

    @Override
    protected void attachBaseContext(Context newBase) {
        super.attachBaseContext(CalligraphyContextWrapper.wrap(newBase));
    }

    @Override
    public void onClick(View view) {
        switch (view.getId()) {
            case R.id.imgMenu:
                break;
        }
    }
}