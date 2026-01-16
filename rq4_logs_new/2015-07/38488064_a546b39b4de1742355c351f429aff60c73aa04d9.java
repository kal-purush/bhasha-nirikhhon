package com.example.arjuns.drums;

import android.app.Activity;
import android.os.Bundle;
import android.view.Menu;
import android.widget.FrameLayout;

/**
 * Created by arjuns on 7/3/2015.
 */
public class ElectroDrums extends Activity {

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_electro_drums);
        FrameLayout myFrameLayout = (FrameLayout)findViewById(R.id.electroFrame);
        myFrameLayout.setOnTouchListener((ElectroDrumsBitmap)findViewById(R.id.img_view));
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.menu_main, menu);
        return true;
    }

}