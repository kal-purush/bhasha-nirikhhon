package com.stfalcon.whoisthere;

/**
 * Created by root on 29.04.15.
 */
import android.app.Activity;

import com.octo.android.robospice.SpiceManager;


public abstract class BaseSpiceActivity extends Activity {
    private SpiceManager spiceManager = new SpiceManager(SampleSpiceService.class);

    @Override
    protected void onStart() {
        spiceManager.start(this);
        super.onStart();
    }

    @Override
    protected void onStop() {
        spiceManager.shouldStop();
        super.onStop();
    }

    protected SpiceManager getSpiceManager() {
        return spiceManager;
    }

}