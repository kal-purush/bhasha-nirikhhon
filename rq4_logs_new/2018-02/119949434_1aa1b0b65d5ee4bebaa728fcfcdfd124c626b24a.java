package com.kth.barfinder.barfinder_native;

import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.widget.Button;
import android.widget.ToggleButton;

import java.util.Arrays;

public class FilterActivity extends AppCompatActivity {

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_filter);

        ToggleButton TglBtn_Beer = (ToggleButton) findViewById(R.id.TglBtn_Beer);
        ToggleButton TglBtn_Beer_2 = (ToggleButton) findViewByid(R.id.TglBtn_Beer_2); // this will go later


        //this has to be changed! just for now
        boolean FilterValues = TglBtn_Beer.isChecked();

    

    }


    // here the filters are fiven out to the mapsActivit
    public Boolean[] getFilterValues()
    {
        Boolean[] FilterValues = new Boolean[2];
        Arrays.fill(FilterValues, Boolean.FALSE);


    }



}