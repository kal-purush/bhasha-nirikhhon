package com.example.arjuns.drums;

import android.app.Activity;
import android.os.Bundle;
import android.view.Menu;
import android.widget.FrameLayout;

/**
 * Created by arjuns on 7/3/2015.
 */
public class RealDrums extends Activity {

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_real_drums);
        FrameLayout myFrameLayout = (FrameLayout)findViewById(R.id.realFrame);
        myFrameLayout.setOnTouchListener((RealDrumsBitmap)findViewById(R.id.img_view));
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.menu_main, menu);
        return true;
    }

}