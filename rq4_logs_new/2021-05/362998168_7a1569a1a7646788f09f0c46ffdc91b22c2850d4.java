package com.example.safety_qr.activity;

import android.content.Intent;
import android.graphics.Color;
import android.net.Uri;
import android.os.Bundle;
import android.view.View;
import android.widget.Button;

import androidx.appcompat.app.AppCompatActivity;

import com.example.safety_qr.R;
import com.github.mikephil.charting.charts.PieChart;
import com.github.mikephil.charting.data.Entry;
import com.github.mikephil.charting.data.PieData;
import com.github.mikephil.charting.data.PieDataSet;
import com.github.mikephil.charting.utils.ColorTemplate;

import java.util.ArrayList;
import java.util.List;

public class Detail_Activity extends AppCompatActivity {

    @Override
    protected void onCreate(Bundle savedInstanceState) {

        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_detail);

        Intent detailIntent = getIntent();
        // **** 악성 : malicious // 백신 수 : total
        final int malicious = detailIntent.getIntExtra("malicious", 1);
        final int total = detailIntent.getIntExtra("total", 1);
        final String url = detailIntent.getStringExtra("url");


        //원형차트
        PieChart pieChart = findViewById(R.id.picChart);
        ArrayList malignity = new ArrayList();
        malignity.add(new Entry(malicious, 0));
        malignity.add(new Entry(total - malicious, 1));

        PieDataSet dataSet = new PieDataSet(malignity, "악성 수치");
        ArrayList result = new ArrayList();
        result.add("악성 검출");
        result.add("악성 미검출");
        dataSet.setValueTextSize(15);

        PieData data = new PieData(result, dataSet);

        ArrayList color = new ArrayList();
        color.add(0xffe84c3d);
        color.add(0xffe0e0e0);


        pieChart.setData(data);
        dataSet.setColors(color);
        pieChart.setHoleColor(0xff17375e);
        pieChart.setCenterText(malicious+" / "+total);
        pieChart.setCenterTextSize(27);
        pieChart.setCenterTextColor(0xffe0e0e0);
        pieChart.animateXY(5000, 5000);




        Button ok_button = findViewById(R.id.ok_button);
        Button go_button = findViewById(R.id.go_button);


        //접속하기
        ok_button.setOnClickListener(new View.OnClickListener(){
            @Override
            public void onClick(View view){
                if(!url.contains("http")){
                    String http = "http://";
                    http = http.concat(url);
                    Intent intent = new Intent(Intent.ACTION_VIEW, Uri.parse(http));
                    startActivity(intent);
                }
                else{
                    Intent intent = new Intent(Intent.ACTION_VIEW, Uri.parse(url));
                    startActivity(intent);
                }

            }
        });

        //바이러스토탈가서 분석하기
        go_button.setOnClickListener(new View.OnClickListener(){
            @Override
            public void onClick(View view){
                //바이러스토탈로가는 링크 연결해야함
                Intent intent = new Intent(Intent.ACTION_VIEW, Uri.parse(url));
                startActivity(intent);
            }
        });

    }

}