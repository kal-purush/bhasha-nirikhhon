package jp.co.megachips.sensorlist;

import android.app.Activity;
import android.hardware.SensorEvent;
import android.hardware.TriggerEvent;
import android.widget.TextView;

/**
 * Created by shingo on 2016/06/02.
 */
public class SensorDisplayAccel extends SensorDisplayBase {

    TextView timestampTextView;
    TextView valueTextView[] = new TextView[3];

    @Override
    public void setUI(Activity activity) {
        timestampTextView = (TextView)activity.findViewById(R.id.accelTimestampTextView);

        valueTextView[0] = (TextView)activity.findViewById(R.id.accelTextView0);
        valueTextView[1] = (TextView)activity.findViewById(R.id.accelTextView1);
        valueTextView[2] = (TextView)activity.findViewById(R.id.accelTextView2);
    }

    @Override
    public void display(SensorEvent event){
        timestampTextView.setText(String.valueOf(event.timestamp));
        for(int i = 0; i < valueTextView.length; i++) {
            valueTextView[i].setText(String.format("%.5f", event.values[i]));
        }
    }

    @Override
    public void display(TriggerEvent event){}
}