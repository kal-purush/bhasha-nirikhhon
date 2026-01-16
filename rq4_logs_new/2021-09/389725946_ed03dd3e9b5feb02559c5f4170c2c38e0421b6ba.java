package com.example.rewirebluetoothforcesensor;

import android.os.Bundle;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ProgressBar;
import android.widget.TextView;

import androidx.fragment.app.Fragment;

// Feet
public class OverviewFragment extends DataViewFragment {
    TextView[] pads;
    String[] sensorData;
    int totalCycles;

    public int LeftNoWeightTimeCount = 0;
    public int LeftNoWeightValue = 0;
    public int RightNoWeightTimeCount = 0;
    public int RightNoWeightValue = 0;

    double[] lh_arr = new double[4];
    double[] lo_arr = new double[4];
    double[] li_arr = new double[4];
    double[] rh_arr = new double[4];
    double[] ro_arr = new double[4];
    double[] ri_arr = new double[4];

    TextView padLtotal;
    TextView padRtotal;
    ProgressBar progbar;

    public OverviewFragment(){
        super(R.layout.overview_fragment);
    }

    @Override
    public View onCreateView(LayoutInflater inflater, ViewGroup container,
                             Bundle savedInstanceState) {
        ViewGroup rootView = (ViewGroup) inflater.inflate(
                R.layout.overview_fragment, container, false);

        pads = new TextView[]{rootView.findViewById(R.id.pad0),
                rootView.findViewById(R.id.pad1),
                rootView.findViewById(R.id.pad2),
                rootView.findViewById(R.id.pad3),
                rootView.findViewById(R.id.pad4),
                rootView.findViewById(R.id.pad5)};

        padLtotal = rootView.findViewById(R.id.padLtot);
        padRtotal = rootView.findViewById(R.id.padRtot);
        progbar = rootView.findViewById(R.id.progressBar);

        sensorData = new String[6];
        totalCycles = 0;

        return rootView;
    }


    public void putSensorData(Bundle args){
        for(int i=0; i<6; i++) {
            sensorData = args.getStringArray("sensorData");
            totalCycles = args.getInt("totalCycles");
        }
    }

    public void update(){
        for(int i=0; i<6; i++){
            pads[i].setText(sensorData[i]);
        }

        double lh_val = Double.parseDouble(sensorData[0]);
        double lo_val = Double.parseDouble(sensorData[1]);
        double li_val = Double.parseDouble(sensorData[2]);
        double rh_val = Double.parseDouble(sensorData[3]);
        double ro_val = Double.parseDouble(sensorData[4]);
        double ri_val = Double.parseDouble(sensorData[5]);

        //this is the spot where the 3 point moving average goes

        //time to call upon the aid of my trusty friend, calculatron
        lh_arr[2]=lh_val; //set new value to third spot in array
        lh_arr = calculatron(lh_arr); // shift stuff back a spot, will get [val, val, 0, avg]
        //since all the arrays start populated with 0s, calculatron should just work
        pads[0].setText(String.format("%.2f", lh_arr[3])); // print avg to left heel, round it

        lo_arr[2] = lo_val;
        lo_arr = calculatron(lo_arr);
        pads[1].setText(String.format("%.2f", lo_arr[3]));

        li_arr[2] = li_val;
        li_arr = calculatron(li_arr);
        pads[2].setText(String.format("%.2f", li_arr[3]));

        rh_arr[2] = rh_val;
        rh_arr = calculatron(rh_arr);
        pads[3].setText(String.format("%.2f", rh_arr[3]));

        ro_arr[2] = ro_val;
        ro_arr = calculatron(ro_arr);
        pads[4].setText(String.format("%.2f", ro_arr[3]));

        ri_arr[2] = ri_val;
        ri_arr = calculatron(ri_arr);
        pads[5].setText(String.format("%.2f", ri_arr[3]));


        double leftval = li_val+lo_val+lh_val;
        double rightval = rh_val+ro_val+ri_val;

        if (leftval == 0 && rightval == 0)
        {progbar.setProgress(50);}
        else {
            int barVal = (int) Math.round(100 * (leftval / (leftval + rightval)));
            progbar.setProgress(barVal);
        }

        padLtotal.setText((String.format("%.2f", leftval)));
        padRtotal.setText((String.format("%.2f", rightval)));

    }

    public double[] calculatron(double[] values)
    {
        // the infinitely wise calculatron takes in an array, averages it,
        // and shifts some stuff around to make a new array
        double first = values[0];
        double second = values[1];
        double third = values[2];
        double placeholder = 0;
        double printout = ((first+second+third)/3);

        values[0] = second;
        values[1] = third;
        values[2] = placeholder;
        values[3] = printout;
        return values;

    }


}