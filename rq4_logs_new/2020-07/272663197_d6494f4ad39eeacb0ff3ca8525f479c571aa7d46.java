package com.nhom08.doanlaptrinhandroid;

import androidx.appcompat.app.AppCompatActivity;

import android.graphics.Color;
import android.os.Bundle;
import android.util.Log;
import android.view.View;
import android.widget.ImageButton;
import android.widget.Toast;

import com.android.volley.Request;
import com.android.volley.RequestQueue;
import com.android.volley.Response;
import com.android.volley.VolleyError;
import com.android.volley.toolbox.JsonObjectRequest;
import com.android.volley.toolbox.Volley;
import com.github.mikephil.charting.charts.BarChart;
import com.github.mikephil.charting.charts.PieChart;
import com.github.mikephil.charting.components.Legend;
import com.github.mikephil.charting.data.BarData;
import com.github.mikephil.charting.data.BarDataSet;
import com.github.mikephil.charting.data.BarEntry;
import com.github.mikephil.charting.data.PieData;
import com.github.mikephil.charting.data.PieDataSet;
import com.github.mikephil.charting.data.PieEntry;
import com.github.mikephil.charting.utils.ColorTemplate;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;

public class Chart extends AppCompatActivity {

    PieChart pieChart;
    int[] colorClassArray = new int[]{Color.RED, Color.YELLOW, Color.GREEN, Color.BLUE, Color.DKGRAY, Color.CYAN, Color.LTGRAY, Color.YELLOW};
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_chart);

        findViewById(R.id.btn_bieudo).setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                DrawPieChart();
            }
        });


        pieChart = findViewById(R.id.PieChart);
        DrawPieChart();

    }
    private void DrawPieChart()
    {
        RequestQueue requestQueue = Volley.newRequestQueue(this);
        JsonObjectRequest request = new JsonObjectRequest(Request.Method.GET, getString(R.string.json_count_post), null,
                new Response.Listener<JSONObject>() {
                    @Override
                    public void onResponse(JSONObject response) {
                        try {
                            final ArrayList<PieEntry> datas = new ArrayList<>();
                            JSONArray jsonArray = response.getJSONArray("statistic");
                            for (int i = 0; i < jsonArray.length(); i++){
                                JSONObject statistic = jsonArray.getJSONObject(i);
                                //dataVals.add(new PieEntry(statistic.getInt("SL"),statistic.getString("term_taxonomy_id")));
                                int vl = Integer.parseInt(statistic.getString("SL"));
                                String dt = statistic.getString("name");
                                if(!dt.equals("Menu")){
                                    datas.add(new PieEntry(vl,dt));
                                }
                                PieDataSet pieDataSet = new PieDataSet(datas,"THỐNG KÊ BÀI VIẾT THEO DANH MỤC");
                                //pieDataSet.setColors(colorClassArray);
                                pieDataSet.setColors(ColorTemplate.PASTEL_COLORS);

                                PieData pieData = new PieData(pieDataSet);
                                pieChart.setData(pieData);
                                pieChart.setUsePercentValues(true);
                                pieChart.setCenterText("Danh Mục");
                                pieChart.setCenterTextSize(30);
                                pieDataSet.setFormSize(100);
                                pieDataSet.setValueTextColor(Color.BLACK);
                                pieDataSet.setValueTextSize(16);

                                Legend legend = pieChart.getLegend();
                                legend.setForm(Legend.LegendForm.CIRCLE);
                                legend.setTextSize(12);
                                legend.setFormSize(20);
                                legend.setFormToTextSpace(2);
                            }

                        } catch (JSONException e) {
                            e.printStackTrace();
                        }
                    }
                }, new Response.ErrorListener() {
            @Override
            public void onErrorResponse(VolleyError error) {
                error.printStackTrace();
            }
        });
        requestQueue.add(request);
        pieChart.invalidate();
        pieChart.setNoDataText("");

    }
}