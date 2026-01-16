package com.foo.umbrella;

import android.content.Context;
import android.view.View;
import android.view.ViewGroup;
import android.widget.BaseAdapter;
import android.widget.GridView;
import android.widget.TextView;

import com.foo.umbrella.data.model.HourlyForecast;

import java.util.ArrayList;

/**
 * Created by Hector Vera on 9/9/2017.
 */

public class ListAdapter extends BaseAdapter {
    private Context context;
    private ArrayList<ArrayList<HourlyForecast>> hourForecastLists;
    private String unit;

    public ListAdapter(Context context, ArrayList<ArrayList<HourlyForecast>> hourForecastLists, String unit) {
        this.context = context;
        this.hourForecastLists = hourForecastLists;
        this.unit = unit;
    }

    @Override
    public int getCount() {
        return hourForecastLists.size();
    }

    @Override
    public ArrayList<HourlyForecast> getItem(int position) {
        return hourForecastLists.get(position);
    }

    @Override
    public long getItemId(int position) {
        return position;
    }

    @Override
    public View getView(int position, View convertView, ViewGroup parent) {
        View view = View.inflate(context, R.layout.container_view, null);

        TextView dateText = (TextView) view.findViewById(R.id.dateText);
        GridView gridView = (GridView) view.findViewById(R.id.gridView);

        ArrayList<HourlyForecast> list = hourForecastLists.get(position);
        if(position == 0){
            dateText.setText("Today " + list.get(0).getFCTTIME().getMonAbbrev()
                + " " + list.get(0).getFCTTIME().getMdayPadded()
                + " " + list.get(0).getFCTTIME().getYear());
        }else if( position == 1){
            dateText.setText("Tomorrow " + list.get(0).getFCTTIME().getMonAbbrev()
                    + " " + list.get(0).getFCTTIME().getMdayPadded()
                    + " " + list.get(0).getFCTTIME().getYear());
        }else{
            dateText.setText(list.get(0).getFCTTIME().getMonAbbrev()
                    + " " + list.get(0).getFCTTIME().getMdayPadded()
                    + " " + list.get(0).getFCTTIME().getYear());
        }
        GridAdapter gridAdapter = new GridAdapter(context, list, unit);
        gridView.setAdapter(gridAdapter);
        return view;
    }
}