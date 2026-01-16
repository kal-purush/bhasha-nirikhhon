package com.foo.umbrella.ui.views;

import android.content.Context;
import android.graphics.Bitmap;
import android.graphics.drawable.Drawable;
import android.widget.ImageView;
import android.widget.LinearLayout;
import android.widget.TextView;



public class HourlyView extends LinearLayout {

    private TextView hour, temperature;
    private ImageView imageView;

    public HourlyView(Context context) {
        super(context);
        hour = new TextView(context);
        temperature = new TextView(context);
        imageView = new ImageView(context);
        this.setOrientation(VERTICAL);

        addView(hour);
        addView(imageView);
        addView(temperature);
    }

    public void setHour(String hour){
        this.hour.setText(hour);
    }

    public String getHourText(){
        return this.hour.getText().toString();
    }

    public void setTemperature(String temperature){
        this.temperature.setText(temperature);
    }

    public String getTemperatureText(){
        return  this.temperature.getText().toString();
    }

    public void setImageView(Drawable imageD){
        this.imageView.setImageDrawable(imageD);
    }

    public void setImageView(Bitmap bm){
        this.imageView.setImageBitmap(bm);
    }
}