package com.lifuz.testdraw;

import android.app.Activity;
import android.content.Context;
import android.graphics.Typeface;
import android.os.Bundle;
import android.util.Log;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.BaseAdapter;
import android.widget.ListView;
import android.widget.TextView;
import android.widget.Toast;

import com.android.volley.RequestQueue;
import com.android.volley.Response;
import com.android.volley.VolleyError;
import com.android.volley.toolbox.JsonArrayRequest;
import com.android.volley.toolbox.Volley;
import com.github.mikephil.charting.charts.BarChart;
import com.github.mikephil.charting.components.Legend;
import com.github.mikephil.charting.components.XAxis;
import com.github.mikephil.charting.components.YAxis;
import com.github.mikephil.charting.data.BarData;
import com.github.mikephil.charting.data.BarDataSet;
import com.github.mikephil.charting.data.BarEntry;
import com.github.mikephil.charting.data.Entry;
import com.github.mikephil.charting.listener.OnChartValueSelectedListener;
import com.github.mikephil.charting.utils.Highlight;
import com.lifuz.testdraw.bean.Chart;
import com.lifuz.testdraw.bean.Work;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 作者：李富 on 2015/8/31.
 * 邮箱：lifuzz@163.com
 */
public class MPABarChart extends Activity implements OnChartValueSelectedListener {

    private BarChart mChart;
    private List<Chart> charts;
    private RequestQueue queue;

    private Typeface mTf;

    private List<Work> works;
    private ListView mp_list;

    private MyAdapter adapter;


    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);

        setContentView(R.layout.mpa_barchart);

        mp_list = (ListView) findViewById(R.id.mp_list);

        works = new ArrayList<>();
        Work work = null;

        for (int i = 0;i <5;i++) {
            work = new Work();
            work.setName("张" + (i+1));
            work.setBj_name("部件" + (i + 1));
            work.setCount(1000 + i + "");
            work.setPass(950 + i + "");
            works.add(work);

        }

        mChart = (BarChart) findViewById(R.id.mpa_bar);
        mTf = Typeface.createFromAsset(getAssets(), "OpenSans-Regular.ttf");

        mChart.setDrawBarShadow(false);
        mChart.setDrawValueAboveBar(true);
//        mChart.setOnChartValueSelectedListener(this);

        mChart.setDescription("");
        // if more than 60 entries are displayed in the chart, no values will be
        // drawn
        mChart.setMaxVisibleValueCount(60);

        mChart.setNoDataText("");
        mChart.setBackgroundColor(getResources().getColor(R.color.mp_back));

        // scaling can now only be done on x- and y-axis separately
        mChart.setPinchZoom(false);

        // draw shadows for each bar that show the maximum value
        // mChart.setDrawBarShadow(true);

        // mChart.setDrawXLabels(false);

        mChart.setDrawGridBackground(false);
        // mChart.setDrawYLabels(false);

//        mTf = Typeface.createFromAsset(getAssets(), "OpenSans-Regular.ttf");

        XAxis xAxis = mChart.getXAxis();
        xAxis.setPosition(XAxis.XAxisPosition.BOTTOM);
        xAxis.setTypeface(mTf);
        xAxis.setDrawGridLines(false);
        xAxis.setSpaceBetweenLabels(2);
        xAxis.setDrawAxisLine(false);


//        mChart.setScrollContainer(false);

        //使图表不能缩放
        mChart.setScaleEnabled(false);

//        mChart.setFilterTouchesWhenObscured(false);

//        ValueFormatter custom = new MyValueFormatter();
//
        YAxis leftAxis = mChart.getAxisLeft();
        leftAxis.setDrawAxisLine(false);
        leftAxis.setDrawGridLines(false);
        leftAxis.setDrawLabels(false);

        //设置双击不进行缩放
        mChart.setDoubleTapToZoomEnabled(false);


//        mChart.setScaleX(0);


//        leftAxis.setTypeface(mTf);
//        leftAxis.setLabelCount(8);
//
//        leftAxis.setValueFormatter(custom);
//        leftAxis.setPosition(YAxis.YAxisLabelPosition.OUTSIDE_CHART);
//        leftAxis.setSpaceTop(15f);
//
        YAxis rightAxis = mChart.getAxisRight();
        rightAxis.setDrawLabels(false);
        rightAxis.setDrawAxisLine(false);
        rightAxis.setDrawGridLines(false);
//        rightAxis.setDrawGridLines(false);
//        rightAxis.setTypeface(mTf);
//        rightAxis.setLabelCount(8);
//        rightAxis.setValueFormatter(custom);
//        rightAxis.setSpaceTop(15f);


//        l.setPosition(Legend.LegendPosition.BELOW_CHART_LEFT);
//        l.setForm(Legend.LegendForm.SQUARE);
//        l.setFormSize(9f);
//        l.setTextSize(11f);
//        l.setXEntrySpace(4f);
        // l.setExtra(ColorTemplate.VORDIPLOM_COLORS, new String[] { "abc",
        // "def", "ghj", "ikl", "mno" });
        // l.setCustom(ColorTemplate.VORDIPLOM_COLORS, new String[] { "abc",
        // "def", "ghj", "ikl", "mno" });

        // mChart.setDrawLegend(false);

        init();

    }

    private void init() {
        queue = Volley.newRequestQueue(this);


        JsonArrayRequest jsonArrayRequest = new JsonArrayRequest(getResources().getString(R.string.count_url),
                new Response.Listener<JSONArray>() {
                    @Override
                    public void onResponse(JSONArray jsonArray) {

                        charts = new ArrayList<>();
                        Chart chart = null;

                        for (int i = 0; i < jsonArray.length(); i++) {

                            chart = new Chart();

                            try {
                                JSONObject object = jsonArray.getJSONObject(i);

                                chart.setDate(object.getString("date"));
                                chart.setCount(object.getString("count"));
                                charts.add(chart);

                            } catch (JSONException e) {
                                e.printStackTrace();
                            }

                        }

                        Collections.reverse(charts);
                        Log.i("tag", charts.toString());

                        setdate();
                        setList();

                    }
                }, new Response.ErrorListener() {
            @Override
            public void onErrorResponse(VolleyError volleyError) {

            }
        });

        queue.add(jsonArrayRequest);
    }

    private void setList() {

        adapter = new MyAdapter(getApplicationContext(),works);
        mp_list.setAdapter(adapter);

    }

    @Override
    public void onValueSelected(Entry entry, int i, Highlight highlight) {
//        Toast.makeText(MPABarChart.this,i + "",Toast.LENGTH_SHORT).show();

        if (entry == null)
            return;

//        RectF bounds = mChart.getBarBounds((BarEntry)  entry);
//
//        PointF position = mChart.getPosition( entry, YAxis.AxisDependency.LEFT);

        Toast.makeText(MPABarChart.this, entry.getVal() + ":" + entry.getXIndex(), Toast.LENGTH_SHORT).show();
    }

    @Override
    public void onNothingSelected() {

    }

    private void setdate() {

        ArrayList<String> xvals = new ArrayList<>();
        for (Chart chart : charts) {
            xvals.add(chart.getDate());
        }

        ArrayList<BarEntry> yvals = new ArrayList<>();
        for (int i = 0; i < charts.size(); i++) {
            yvals.add(new BarEntry(Float.parseFloat(charts.get(i).getCount()), i));
        }

        BarDataSet set = new BarDataSet(yvals, "tongji");
        set.setBarSpacePercent(25f);

        ArrayList<BarDataSet> barDataSets = new ArrayList<>();
        barDataSets.add(set);
        BarData data = new BarData(xvals, barDataSets);

        data.setValueTextSize(10f);
        data.setValueTypeface(mTf);

        mChart.setData(data);

        Legend l = mChart.getLegend();
        l.setEnabled(false);
//        mChart.animate();


        mChart.setOnChartValueSelectedListener(this);

        mChart.animateX(25000);

    }


    class MyAdapter extends BaseAdapter {

        private LayoutInflater layoutInflater;
        private List<Work> works ;

        public MyAdapter(Context context,List works) {
            this.works = works;
            layoutInflater = LayoutInflater.from(context);

        }

        class WorkItem{
            public TextView name;
            public TextView bj_name;
            public TextView count;
            public TextView pass;
        }

        @Override
        public int getCount() {
            return works.size();
        }

        @Override
        public Object getItem(int position) {
            return position;
        }

        @Override
        public long getItemId(int position) {
            return position;
        }

        @Override
        public View getView(int position, View convertView, ViewGroup parent) {

            WorkItem workItem = null;

            if (convertView == null) {
                workItem = new WorkItem();

                convertView = layoutInflater.inflate(R.layout.mp_item,null);

                workItem.name = (TextView) convertView.findViewById(R.id.mp_name);
                workItem.bj_name = (TextView) convertView.findViewById(R.id.mp_bj);
                workItem.count = (TextView) convertView.findViewById(R.id.mp_count);
                workItem.pass = (TextView) convertView.findViewById(R.id.mp_pass);
                convertView.setTag(workItem);

            } else  {
                workItem = (WorkItem)convertView.getTag();
            }

            workItem.name.setText(works.get(position).getName());
            workItem.bj_name.setText(works.get(position).getBj_name());
            workItem.count.setText(works.get(position).getCount());
            workItem.pass.setText(works.get(position).getPass());

            return convertView;
        }
    }

}