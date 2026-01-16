package com.highcom.ponshu.util;

import android.graphics.Color;

import com.github.mikephil.charting.charts.LineChart;
import com.github.mikephil.charting.components.XAxis;
import com.github.mikephil.charting.components.YAxis;
import com.github.mikephil.charting.data.Entry;
import com.github.mikephil.charting.data.LineData;
import com.github.mikephil.charting.data.LineDataSet;
import com.github.mikephil.charting.formatter.IFillFormatter;
import com.github.mikephil.charting.interfaces.dataprovider.LineDataProvider;
import com.github.mikephil.charting.interfaces.datasets.ILineDataSet;

import java.util.List;

public class LineChartItem {
    private LineChart mLineChart;

    public LineChartItem(LineChart lineChart) {
        mLineChart = lineChart;
    }

    /**
     * ラインチャート画面更新処理
     */
    public void updateLineChart()
    {
        mLineChart.setViewPortOffsets(0, 0, 0, 0);
        mLineChart.setBackgroundColor(Color.rgb(104, 241, 175));

        // no description text
        mLineChart.getDescription().setEnabled(false);

        // enable touch gestures
        mLineChart.setTouchEnabled(true);

        // enable scaling and dragging
        mLineChart.setDragEnabled(true);
        mLineChart.setScaleEnabled(true);

        // if disabled, scaling can be done on x- and y-axis separately
        mLineChart.setPinchZoom(false);

        mLineChart.setDrawGridBackground(false);
        mLineChart.setMaxHighlightDistance(300);

        XAxis x = mLineChart.getXAxis();
        x.setEnabled(false);

        YAxis y = mLineChart.getAxisLeft();
        y.setLabelCount(6, false);
        y.setTextColor(Color.WHITE);
        y.setPosition(YAxis.YAxisLabelPosition.INSIDE_CHART);
        y.setDrawGridLines(false);
        y.setAxisLineColor(Color.WHITE);

        mLineChart.getAxisRight().setEnabled(false);

        mLineChart.getLegend().setEnabled(false);

        mLineChart.animateXY(2000, 2000);

        // don't forget to refresh the drawing
        mLineChart.invalidate();

//        setLineData();
    }

    /**
     * ラインチャートデータ設定処理
     *
     * @param values
     */
    public void setLineData(List<Entry> values) {

//        ArrayList<Entry> values = new ArrayList<>();

//        for (int i = 0; i < count; i++) {
//            float val = (float) (Math.random() * (range + 1)) + 20;
//            values.add(new Entry(i, val));
//        }
//        values.add(new Entry(0, 10));
//        values.add(new Entry(1, 6));
//        values.add(new Entry(2, 4));
//        values.add(new Entry(3, 2));
//        values.add(new Entry(4, 1));

        LineDataSet set1;

        if (mLineChart.getData() != null &&
                mLineChart.getData().getDataSetCount() > 0) {
            set1 = (LineDataSet) mLineChart.getData().getDataSetByIndex(0);
            set1.setValues(values);
            mLineChart.getData().notifyDataChanged();
            mLineChart.notifyDataSetChanged();
        } else {
            // create a dataset and give it a type
            set1 = new LineDataSet(values, "DataSet 1");

            set1.setMode(LineDataSet.Mode.CUBIC_BEZIER);
            set1.setCubicIntensity(0.2f);
            set1.setDrawFilled(true);
            set1.setDrawCircles(false);
            set1.setLineWidth(1.8f);
            set1.setCircleRadius(4f);
            set1.setCircleColor(Color.WHITE);
            set1.setHighLightColor(Color.rgb(244, 117, 117));
            set1.setColor(Color.WHITE);
            set1.setFillColor(Color.WHITE);
            set1.setFillAlpha(100);
            set1.setDrawHorizontalHighlightIndicator(false);
            set1.setFillFormatter(new IFillFormatter() {
                @Override
                public float getFillLinePosition(ILineDataSet dataSet, LineDataProvider dataProvider) {
                    return mLineChart.getAxisLeft().getAxisMinimum();
                }
            });

            // create a data object with the data sets
            LineData data = new LineData(set1);
            data.setValueTextSize(9f);
            data.setDrawValues(false);

            // set data
            mLineChart.setData(data);
        }
    }
}