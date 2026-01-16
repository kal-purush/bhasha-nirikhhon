package com.example.rewirebluetoothforcesensor;

import android.graphics.Color;
import android.os.Bundle;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;

import com.github.mikephil.charting.charts.LineChart;
import com.github.mikephil.charting.components.YAxis;
import com.github.mikephil.charting.data.Entry;
import com.github.mikephil.charting.data.LineData;
import com.github.mikephil.charting.data.LineDataSet;
import com.github.mikephil.charting.interfaces.datasets.ILineDataSet;
import com.github.mikephil.charting.utils.ColorTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

class ProSupFragment extends DataViewFragment {
    int[] sensorDataArr;
    int totalCycles;

    LineChart chart;


    public ProSupFragment(){
        super(R.layout.prosup_fragment);
    }

    @Override
    public View onCreateView(LayoutInflater inflater, ViewGroup container,
                             Bundle savedInstanceState) {
        ViewGroup rootView = (ViewGroup) inflater.inflate(
                R.layout.prosup_fragment, container, false);

        chart = (LineChart) rootView.findViewById(R.id.chart);

        List<Entry> entries = new ArrayList<Entry>();
        LineDataSet dataSet = new LineDataSet(entries, "Label");
        LineData data = new LineData(dataSet);

        chart.setData(data);
        chart.invalidate();

        return rootView;
    }

    public void putSensorData(Bundle args){
        for(int i=0; i<6; i++) {
            sensorDataArr = args.getIntArray("sensorData");
            totalCycles = args.getInt("totalCycles");
        }
    }

    public void update(){
        LineData data = chart.getData();

        if (data != null) {

            ILineDataSet dataSet = data.getDataSetByIndex(0);
            // set.addEntry(...); // can be called as well

            if (dataSet == null) {
                dataSet = createSet();
                data.addDataSet(dataSet);
            }

            data.addEntry(new Entry(dataSet.getEntryCount(), sensorDataArr[1] - sensorDataArr[2]), 0);
            data.notifyDataChanged();

            // let the chart know it's data has changed
            chart.notifyDataSetChanged();

            chart.setVisibleXRangeMaximum(120);

            chart.moveViewToX(data.getEntryCount());

        }
    }


    private LineDataSet createSet() {

        LineDataSet dataSet = new LineDataSet(null, "Dynamic Data");
        dataSet.setAxisDependency(YAxis.AxisDependency.LEFT);
        dataSet.setColor(ColorTemplate.getHoloBlue());
        dataSet.setCircleColor(Color.WHITE);
        dataSet.setLineWidth(2f);
        dataSet.setCircleRadius(4f);
        dataSet.setFillAlpha(65);
        dataSet.setFillColor(ColorTemplate.getHoloBlue());
        dataSet.setHighLightColor(Color.rgb(244, 117, 117));
        dataSet.setValueTextColor(Color.WHITE);
        dataSet.setValueTextSize(9f);
        dataSet.setDrawValues(false);
        return dataSet;
    }
}