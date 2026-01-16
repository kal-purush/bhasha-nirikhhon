package com.example.tanvi.ireport.Activity;

import android.content.Intent;
import android.support.design.widget.FloatingActionButton;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.view.View;
import android.widget.AdapterView;
import android.widget.ListView;
import android.widget.Toast;

import com.example.tanvi.ireport.Model.Report_Item;
import com.example.tanvi.ireport.R;
import com.example.tanvi.ireport.Utility.ReportsAdapter;

import java.util.ArrayList;

public class MyReportActivity extends AppCompatActivity implements AdapterView.OnItemClickListener {


    public static final String[] names = new String[]{"Report 1", "Report 2", "Report 3", "Report 4"};
    public static final String[] status = new String[]{"Still_there", "Completed", "Still_there", "Still_there"};
    public static final String[] dateTime = new String[]{"12/2/2016 9:04PM", "11/25/2016 10.00AM", "5/10/2016 5:00PM", "11/1/2015 2:10PM"};
    public static final Integer[] images = {R.drawable.image1, R.drawable.image2, R.drawable.image3, R.drawable.image4};



    ListView myReportsListView;
    ArrayList<Report_Item> reportsList;
    FloatingActionButton floatingActionButton;


    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_my_report);

        reportsList = new ArrayList<Report_Item>();
        for (int i = 0; i < names.length; i++) {

            Report_Item report_item = new Report_Item(names[i], status[i], dateTime[i], images[i]);
            reportsList.add(report_item);
        }
        myReportsListView = (ListView) findViewById(R.id.listViewReports);
        ReportsAdapter adapter = new ReportsAdapter(this, reportsList);
        myReportsListView.setAdapter(adapter);
        myReportsListView.setOnItemClickListener(this);

        floatingActionButton = (FloatingActionButton) findViewById(R.id.mapButton);
        floatingActionButton.setOnClickListener(new View.OnClickListener(){

            @Override
            public void onClick(View view) {

                Toast.makeText(MyReportActivity.this,"Map view is selected !!", Toast.LENGTH_LONG).show();

                Intent intent = new Intent(MyReportActivity.this, MyReportMapActivity.class);
                startActivity(intent);
            }
        });


    }

    @Override
    public void onItemClick(AdapterView<?> adapterView, View view, int i, long l) {

        Toast.makeText(getApplicationContext(),"Report "+(i+1)+" is clicked", Toast.LENGTH_SHORT).show();

    }
}