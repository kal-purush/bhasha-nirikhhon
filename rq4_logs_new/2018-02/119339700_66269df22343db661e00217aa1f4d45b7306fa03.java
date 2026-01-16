package piosdamian.pl.speedometer;

import android.content.BroadcastReceiver;
import android.content.Context;
import android.content.Intent;
import android.content.IntentFilter;
import android.os.Bundle;
import android.support.annotation.NonNull;
import android.support.annotation.Nullable;
import android.support.v4.app.Fragment;
import android.support.v7.widget.AppCompatRadioButton;
import android.support.v7.widget.AppCompatTextView;
import android.util.Log;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.RadioButton;

import piosdamian.pl.speedometer.service.FloatingWidgetService;
import piosdamian.pl.speedometer.service.GPSService;
import piosdamian.pl.speedometer.service.StoreService;

import static piosdamian.pl.speedometer.service.StoreService.KMH;
import static piosdamian.pl.speedometer.service.StoreService.MPH;

/**
 * Created by Damian Pioś on 30.01.2018.
 */

public class StatsFragment extends Fragment {
    AppCompatTextView time_tv, distance_tv, currentSpeed_tv, maxSpeed_tv, avgSpeed_tv;
    AppCompatRadioButton kmhBtn, mphBtn;


    @Nullable
    @Override
    public View onCreateView(@NonNull LayoutInflater inflater, @Nullable ViewGroup container, @Nullable Bundle savedInstanceState) {
        ViewGroup rootView = (ViewGroup) inflater.inflate(R.layout.fragment_stats, container, false);
        getActivity().registerReceiver(storeReceiver, new IntentFilter(StoreService.STORE_RECEIVER));

        time_tv = rootView.findViewById(R.id.time);
        distance_tv = rootView.findViewById(R.id.distance);
        currentSpeed_tv = rootView.findViewById(R.id.current_speed);
        maxSpeed_tv = rootView.findViewById(R.id.max_speed);
        avgSpeed_tv = rootView.findViewById(R.id.avg_speed);

        kmhBtn = rootView.findViewById(R.id.kmh);
        mphBtn = rootView.findViewById(R.id.mph);

        setUnitChecked();

        kmhBtn.setOnClickListener(onClickListener);
        mphBtn.setOnClickListener(onClickListener);

        rootView.findViewById(R.id.switch_to_widget).setOnClickListener(switchToWidget);

        return super.onCreateView(inflater, container, savedInstanceState);
    }

    private BroadcastReceiver storeReceiver = new BroadcastReceiver() {
        @Override
        public void onReceive(Context context, Intent intent) {
            setView(intent);
        }
    };
    private View.OnClickListener switchToWidget = new View.OnClickListener() {
        @Override
        public void onClick(View v) {
            getActivity().startService(new Intent(getContext().getApplicationContext(), FloatingWidgetService.class));
            getActivity().finish();
        }
    };

    private void setView(Intent intent) {
        resolveBroadcast(intent);
    }

    private void resolveBroadcast(Intent intent) {
        Log.d(StoreService.CURRENT_SPEED, intent.getDoubleExtra(StoreService.CURRENT_SPEED, 0.0) + "");
        time_tv.post(() ->
                time_tv.setText(intent.getLongExtra(StoreService.TIME, 0) / 1000 + "s")
        );
        distance_tv.post(() ->
                distance_tv.setText(String.format(getResources().getConfiguration().locale, "%.02f", intent.getFloatExtra(StoreService.DISTANCE, 0)))
        );
        currentSpeed_tv.post(() ->
                currentSpeed_tv.setText(String.format(getResources().getConfiguration().locale, "%.02f", intent.getDoubleExtra(StoreService.CURRENT_SPEED, 0.0)))
        );
        maxSpeed_tv.post(() ->
                maxSpeed_tv.setText(String.format(getResources().getConfiguration().locale, "%.02f", intent.getDoubleExtra(StoreService.MAX_SPEED, 0.0)))
        );
        avgSpeed_tv.post(() ->
                avgSpeed_tv.setText(String.format(getResources().getConfiguration().locale, "%.02f", intent.getDoubleExtra(StoreService.AVG_SPEED, 0.0)))
        );
    }

    @Override
    public void onDestroy() {
        super.onDestroy();
        getActivity().unregisterReceiver(storeReceiver);
        getActivity().stopService(new Intent(getActivity().getApplicationContext(), GPSService.class));
        getActivity().stopService(new Intent(getActivity().getApplicationContext(), StoreService.class));
    }

    View.OnClickListener onClickListener = new View.OnClickListener() {
        @Override
        public void onClick(View view) {
            switch (view.getId()) {
                case R.id.mph:
                    if (((RadioButton) view).isChecked()) {
                        StoreService.changeUnits(MPH);
                    }
                    break;
                case R.id.kmh:
                    if (((RadioButton) view).isChecked()) {
                        StoreService.changeUnits(KMH);
                    }
                    break;
            }
        }
    };

    private void setUnitChecked() {
        int units = StoreService.getUnits();
        if (units == KMH) {
            kmhBtn.setChecked(true);
            mphBtn.setChecked(false);
        } else {
            kmhBtn.setChecked(false);
            mphBtn.setChecked(true);
        }
    }
}