package com.foo.umbrella;

import android.content.Context;
import android.view.View;
import android.view.ViewGroup;
import android.widget.BaseAdapter;
import android.widget.ImageView;
import android.widget.TextView;

import com.foo.umbrella.R;
import com.foo.umbrella.data.model.HourlyForecast;
import com.foo.umbrella.database.library.Library;

import java.util.ArrayList;

/**
 * Created by Hector Vera on 9/8/2017.
 */

public class GridAdapter extends BaseAdapter {
    private Context context;
    private ArrayList<HourlyForecast> hourlyForecastsList;
    private String unit;

    public GridAdapter(Context context, ArrayList<HourlyForecast> hourlyForecastsList, String unit) {
        this.context = context;
        this.hourlyForecastsList = hourlyForecastsList;
        this.unit = unit;
    }

    @Override
    public int getCount() {
        return hourlyForecastsList.size();
    }

    @Override
    public HourlyForecast getItem(int position) {
        return hourlyForecastsList.get(position);
    }

    @Override
    public long getItemId(int position) {
        return position;
    }

    @Override
    public View getView(int position, View convertView, ViewGroup parent) {
        View view = View.inflate(context, R.layout.hourly_view, null);
        TextView hourText = (TextView) view.findViewById(R.id.hourText);
        TextView hWeatherText = (TextView) view.findViewById(R.id.hWeatherText);
        ImageView imageView = (ImageView) view.findViewById(R.id.imageView);

        HourlyForecast h = hourlyForecastsList.get(position);
        hourText.setText(h.getFCTTIME().getCivil());
        if(unit.equals(Library.FAHRENHEIT)){
            hWeatherText.setText(h.getTemp().getEnglish());
        } else {
            hWeatherText.setText(h.getTemp().getMetric());
        }
        imageView.setImageDrawable(view.getResources().getDrawable(R.drawable.weather_sunny));
        return view;
    }




}