package com.example.application;

import androidx.appcompat.app.AppCompatActivity;

import android.graphics.Color;
import android.os.Bundle;

import com.github.mikephil.charting.charts.BarChart;
import com.github.mikephil.charting.components.XAxis;
import com.github.mikephil.charting.components.YAxis;
import com.github.mikephil.charting.data.BarData;
import com.github.mikephil.charting.data.BarDataSet;
import com.github.mikephil.charting.data.BarEntry;
import com.github.mikephil.charting.data.Entry;
import com.github.mikephil.charting.formatter.IValueFormatter;
import com.github.mikephil.charting.utils.ViewPortHandler;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

public class Main_3_2_2 extends AppCompatActivity {//bar chart期間圖表
    private BarChart mChart;
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_3_2_2);

        mChart=(BarChart)findViewById(R.id.chart1);
        mChart.setBackgroundColor(Color.WHITE);
        mChart.setExtraTopOffset(-30f);
        mChart.setExtraBottomOffset(10f);
        mChart.setExtraLeftOffset(70f);
        mChart.setExtraRightOffset(70f);

        mChart.setDrawBarShadow(false);
        mChart.setDrawValueAboveBar(true);

        mChart.getDescription().setEnabled(false);

        // scaling can now only be done on x- and y-axis separately
        mChart.setPinchZoom(false);

        mChart.setDrawGridBackground(false);

        XAxis xAxis = mChart.getXAxis();
        xAxis.setPosition(XAxis.XAxisPosition.BOTTOM);
//        xAxis.setTypeface(tfRegular);
        xAxis.setDrawGridLines(false);
        xAxis.setDrawAxisLine(false);
        xAxis.setTextColor(Color.LTGRAY);
        xAxis.setTextSize(13f);
        xAxis.setLabelCount(5);
        xAxis.setCenterAxisLabels(true);
        xAxis.setGranularity(1f);

        YAxis left = mChart.getAxisLeft();
        left.setDrawLabels(false);
        left.setSpaceTop(25f);
        left.setSpaceBottom(25f);
        left.setDrawAxisLine(false);
        left.setDrawGridLines(false);
        left.setDrawZeroLine(true); // draw a zero line
        left.setZeroLineColor(Color.GRAY);
        left.setZeroLineWidth(0.7f);
        mChart.getAxisRight().setEnabled(false);
        mChart.getLegend().setEnabled(false);

        // THIS IS THE ORIGINAL DATA YOU WANT TO PLOT
        final List<Data>data=new ArrayList<>();
        data.add(new Data(0f, -224.1f, "11111"));
        data.add(new Data(1f, 238.5f, "2222"));
        data.add(new Data(2f, 1280.1f, "累計餘絀"));
        data.add(new Data(3f, -442.3f, "本日結算"));
        data.add(new Data(4f, -2280.1f, "累計餘絀"));
        xAxis.setValueFormatter(new com.github.mikephil.charting.formatter.ValueFormatter() {
            @Override
            public String getFormattedValue(float value) {
                return data.get(Math.min(Math.max((int) value, 0), data.size()-1)).xAxisValue;
            }
        });
        setData(data);
    }

    private  void  setData(List<Data> dataList){
        ArrayList<BarEntry> values=new ArrayList<>();
        List<Integer> colors=new ArrayList<>();

        int green= Color.rgb(110,190,102);
        int red =Color.rgb(211,87,44);

        for (int i=0;i<dataList.size();i++){
            Data d=dataList.get(i);
            BarEntry entry=new BarEntry(d.xValue,d.yValue);
            values.add(entry);

            if (d.yValue>=0){
                colors.add(red);
            }else {
                colors.add(green);
            }
        }

        BarDataSet set;
        set=new BarDataSet(values,"Values");
        set.setColors(colors);
        set.setValueTextColors(colors);

        BarData data=new BarData(set);
        data.setValueTextSize(15f);

        data.setValueFormatter(new ValueFormatter());
        data.setBarWidth(0.9f);

        mChart.setData(data);
        mChart.invalidate();
    }
    private  class Data{
        public String xAxisValue;
        public float xValue;
        public float yValue;

        public Data(float xValue,float yValue,String xAxisValue){
            this.xAxisValue=xAxisValue;
            this.xValue=xValue;
            this.yValue=yValue;

        }
    }
    private class ValueFormatter extends com.github.mikephil.charting.formatter.ValueFormatter implements IValueFormatter {
        private DecimalFormat mFormat;
        public  ValueFormatter(){
            mFormat=new DecimalFormat("#####.0");
        }
        public String getFormattedValue(float value, Entry entry, int daaSetIndex, ViewPortHandler viewPortHandler){
            return mFormat.format(value);
        }
    }

}