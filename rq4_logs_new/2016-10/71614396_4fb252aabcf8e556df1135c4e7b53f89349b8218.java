package com.hackharvard.smartmusicshuffle.activity;

import android.net.Uri;
import android.support.annotation.IdRes;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;

import com.hackharvard.smartmusicshuffle.R;
import com.hackharvard.smartmusicshuffle.fragment.CameraFragment;
import com.hackharvard.smartmusicshuffle.fragment.SmileFragment;
import com.roughike.bottombar.BottomBar;
import com.roughike.bottombar.OnTabSelectListener;

import butterknife.BindView;
import butterknife.ButterKnife;

public class HomeActivity extends AppCompatActivity implements SmileFragment.OnSmileFragmentInteractionListener, CameraFragment.OnCameraFragmentInteractionListener {

    @BindView(R.id.bottomBar)
    BottomBar bottomBar;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_home);

        ButterKnife.bind(this);

        bottomBar.setOnTabSelectListener(new OnTabSelectListener() {
            @Override
            public void onTabSelected(@IdRes int tabId) {
                if (tabId == R.id.tab_smile) {
                    SmileFragment smileFragment = SmileFragment.newInstance();
                    getSupportFragmentManager().beginTransaction()
                            .replace(R.id.contentContainer, smileFragment).commit();
                } else if (tabId == R.id.tab_camera) {
                    CameraFragment cameraFragment = CameraFragment.newInstance();
                    getSupportFragmentManager().beginTransaction()
                            .replace(R.id.contentContainer, cameraFragment).commit();
                } else if (tabId == R.id.tab_recommendation) {

                }
            }
        });
    }

    @Override
    public void onFragmentInteraction(Uri uri) {

    }
}