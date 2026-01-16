package com.test.murphy.weatherapp;

import android.app.AlertDialog;
import android.app.Fragment;
import android.content.BroadcastReceiver;
import android.content.Context;
import android.content.DialogInterface;
import android.content.Intent;
import android.content.IntentFilter;
import android.graphics.Color;
import android.graphics.Point;
import android.os.Bundle;
import android.support.annotation.NonNull;
import android.support.v4.app.ActivityCompat;
import android.support.v4.content.LocalBroadcastManager;
import android.text.InputType;
import android.view.Gravity;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.EditText;
import android.widget.GridLayout;
import android.widget.ImageView;
import android.widget.LinearLayout;
import android.widget.TextView;
import android.widget.ToggleButton;

import com.crashlytics.android.answers.Answers;
import com.crashlytics.android.answers.CustomEvent;
import com.test.murphy.weatherapp.model.Units;
import com.test.murphy.weatherapp.model.WeatherConditions;
import com.test.murphy.weatherapp.model.WeatherForecast;

import java.text.SimpleDateFormat;
import java.util.Locale;

import butterknife.BindView;
import butterknife.ButterKnife;
import butterknife.OnClick;

import static android.view.ViewGroup.LayoutParams.MATCH_PARENT;


/**
 * A simple {@link Fragment} subclass.
 * Use the {@link CurrentFragment#newInstance} factory method to
 * create an instance of this fragment.
 */
public class CurrentFragment extends Fragment implements ActivityCompat.OnRequestPermissionsResultCallback {

    @BindView(R.id.temperatureText) TextView temperatureText;
    @BindView(R.id.conditionsText) TextView conditionsText;
    @BindView(R.id.locationText) TextView locationText;
    @BindView(R.id.conditionsImage) ImageView conditionsImage;
    @BindView(R.id.unitsToggle) ToggleButton unitsToggle;
    @BindView(R.id.forecastGrid) GridLayout gridLayout;

    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("h aa", Locale.US);

    private BroadcastReceiver mBroadcastReceiver;

    public CurrentFragment() {
        // Required empty public constructor
    }

    public static CurrentFragment newInstance() {
        CurrentFragment fragment = new CurrentFragment();
        return fragment;
    }

    @Override
    public View onCreateView(LayoutInflater inflater, ViewGroup container,
                             Bundle savedInstanceState) {
        ViewGroup rootView = (ViewGroup) inflater.inflate(R.layout.fragment_current, container, false);
        ButterKnife.bind(this, rootView);

        mBroadcastReceiver = new BroadcastReceiver() {
            @Override
            public void onReceive(Context context, final Intent intent) {
                //This should be sent from a background thread. Return to main before updating UI
                getActivity().runOnUiThread(new Runnable() {
                    @Override
                    public void run() {
                        if (intent.getAction().equals("android.intent.action.WEATHER_CHANGED")) {
                            updateConditionsLayout();
                            updateConditionsImage();
                            updateForecastLayout();
                        }
                    }
                });
            }
        };

        IntentFilter filter = new IntentFilter("android.intent.action.WEATHER_CHANGED");
        LocalBroadcastManager.getInstance(WeatherApp.getContext()).registerReceiver(mBroadcastReceiver, filter);

        // Inflate the layout for this fragment
        return rootView;

    }

    private void updateConditionsLayout() {
        WeatherConditions weatherConditions = WeatherManager.getInstance().conditions;
        if (weatherConditions == null) {
            return;
        }

        Units units = WeatherManager.getInstance().getUnits();

        temperatureText.setText(String.valueOf(weatherConditions.currentTemperature) + units.getText());
        conditionsText.setText(weatherConditions.currentConditions);
        locationText.setText(weatherConditions.location);
        updateConditionsImage();
    }

    //Update the conditions image based on the condition code
    //Images are sourced from Icons8 http://icons8.com under CC-BY ND 3.0 license
    private void updateConditionsImage() {
        WeatherConditions weatherConditions = WeatherManager.getInstance().conditions;
        if (weatherConditions == null) {
            return;
        }

        //If image was previously hidden, show it
        if (conditionsImage.getImageAlpha() == 0) {
            conditionsImage.setImageAlpha(1);
        }
        if (isBetween(weatherConditions.conditionCode, 200, 299)) {
            //Storm
            conditionsImage.setImageResource(R.drawable.icons8storm);
        } else if (isBetween(weatherConditions.conditionCode, 300, 599)) {
            //Drizzle\Rain
            conditionsImage.setImageResource(R.drawable.icons8rain);
        } else if (isBetween(weatherConditions.conditionCode, 600, 699)) {
            //Snow
            conditionsImage.setImageResource(R.drawable.icons8snow);
        } else if (isBetween(weatherConditions.conditionCode, 700, 799)) {
            //Atmosphere - Mist, smoke, etc
            conditionsImage.setImageResource(R.drawable.icons8haze);
        }  else if (weatherConditions.conditionCode == 800) {
            //Clear sky
            conditionsImage.setImageResource(R.drawable.icons8sun);
        } else if (isBetween(weatherConditions.conditionCode, 801, 899)) {
            //Clouds
            conditionsImage.setImageResource(R.drawable.icons8clouds);
        } else if (isBetween(weatherConditions.conditionCode, 900, 999)) {
            //Severe - No icons for these, hide the icon
            conditionsImage.setImageAlpha(0);
        }

        conditionsImage.setContentDescription(weatherConditions.currentConditions);
    }

    private void updateForecastLayout() {
        WeatherForecast weatherForecast = WeatherManager.getInstance().forecast;
        if (weatherForecast == null) {
            return;
        }

        Units units = WeatherManager.getInstance().getUnits();

        int screenWidth = getScreenWidth();

        //Use lesser of 3 or the size of the forecast
        int columnCount = weatherForecast.forecast.length < 3 ? weatherForecast.forecast.length : 3;

        gridLayout.setColumnCount(columnCount);
        gridLayout.setRowCount(3);
        gridLayout.setOrientation(GridLayout.VERTICAL);
        gridLayout.removeAllViewsInLayout();

        for (int i = 0; i < 3; i++) {
            WeatherConditions weather = weatherForecast.forecast[i];


            //Time
            GridLayout.LayoutParams first = new GridLayout.LayoutParams(GridLayout.spec(0), GridLayout.spec(i));
            first.width = screenWidth / 3;
            first.height = gridLayout.getHeight() / 3;

            TextView forecastTime = new TextView(WeatherApp.getContext());
            String formattedDate = dateFormat.format(weather.date);
            forecastTime.setText(formattedDate);
            forecastTime.setTextColor(Color.WHITE);
            forecastTime.setLayoutParams(first);
            forecastTime.setGravity(Gravity.CENTER);

            gridLayout.addView(forecastTime, first);


            //Temp
            GridLayout.LayoutParams second = new GridLayout.LayoutParams(GridLayout.spec(1), GridLayout.spec(i));
            second.width = screenWidth / 3;
            second.height = gridLayout.getHeight() / 3;

            TextView forecastTemp = new TextView(WeatherApp.getContext());
            forecastTemp.setText(weather.currentTemperature.toString() + units.getText());
            forecastTemp.setTextColor(Color.WHITE);
            forecastTemp.setLayoutParams(second);
            forecastTemp.setGravity(Gravity.CENTER);

            gridLayout.addView(forecastTemp, second);


            //Conditions
            GridLayout.LayoutParams third = new GridLayout.LayoutParams(GridLayout.spec(2), GridLayout.spec(i));
            third.width = screenWidth / 3;
            third.height = gridLayout.getHeight() / 3;


            TextView forecastConditions = new TextView(WeatherApp.getContext());
            forecastConditions.setText(weather.currentConditions);
            forecastConditions.setTextColor(Color.WHITE);
            forecastConditions.setLayoutParams(third);
            forecastConditions.setGravity(Gravity.CENTER);

            gridLayout.addView(forecastConditions, third);
        }
    }

    @OnClick(R.id.aboutButton)
    public void aboutButtonTapped() {
        AlertDialog.Builder builder = new AlertDialog.Builder(WeatherApp.getContext());
        builder.setTitle("About");
        builder.setMessage("Weather icons courtesy of Icons8 under CC-BY ND 3.0 license.\nhttps://icons8.com/");
        builder.setPositiveButton("OK",  new DialogInterface.OnClickListener() {
            @Override
            public void onClick(DialogInterface dialog, int which) {
                //TODO: Do nothing
            }
        });
        builder.show();
    }

    @OnClick(R.id.unitsToggle)
    public void unitsToggleTapped() {
        if (!isUnitsToggleChecked()) {
            WeatherManager.getInstance().setUnits(Units.Fahrenheit);
        } else {
            WeatherManager.getInstance().setUnits(Units.Celsius);
        }
    }

    @OnClick(R.id.locationButton)
    public void locationButtonTapped() {
        showLocationAlert();
    }

    public boolean isUnitsToggleChecked() {
        return unitsToggle.isChecked();
    }

    private void showLocationAlert() {
        AlertDialog.Builder builder = new AlertDialog.Builder(getActivity());
        builder.setTitle("Change Location");

        LinearLayout layout = new LinearLayout(getActivity());
        layout.setOrientation(LinearLayout.VERTICAL);
        LinearLayout.LayoutParams params = new LinearLayout.LayoutParams(
                MATCH_PARENT, LinearLayout.LayoutParams.WRAP_CONTENT);
        params.setMargins(80, 0, 80, 0); //TODO: How far to inset?


        final EditText input = new EditText(getActivity());
        input.setInputType(InputType.TYPE_CLASS_NUMBER);
        layout.addView(input, params);

        input.requestFocus();

        builder.setView(layout);
        builder.setPositiveButton("OK", new DialogInterface.OnClickListener() {
            @Override
            public void onClick(DialogInterface dialog, int which) {
                Answers.getInstance().logCustom(new CustomEvent("Change Location Tapped").putCustomAttribute("Action","OK"));

                //TODO: Edit checks on zip to ensure 5 digits
                WeatherManager.getInstance().setZip(input.getText().toString());
            }
        });

        builder.setNeutralButton("Use Current Location", new DialogInterface.OnClickListener() {
            @Override
            public void onClick(DialogInterface dialog, int which) {
                Answers.getInstance().logCustom(new CustomEvent("Change Location Tapped").putCustomAttribute("Action","Current Location"));

                //Resolve GPS\Network location
                LocationUtils.getInstance().resolveLocation(getActivity());

                WeatherManager.getInstance().setZip(LocationUtils.getInstance().getZip());
            }
        });

        builder.setNegativeButton("Cancel", new DialogInterface.OnClickListener() {
            @Override
            public void onClick(DialogInterface dialog, int which) {
                Answers.getInstance().logCustom(new CustomEvent("Change Location Tapped").putCustomAttribute("Action","Cancel"));

                dialog.cancel();
            }
        });

        builder.show();
    }

    @Override
    public void onRequestPermissionsResult(int requestCode, @NonNull String[] permissions, @NonNull int[] grantResults) {
        super.onRequestPermissionsResult(requestCode, permissions, grantResults);

        //Check if both requested permissions were granted. If denied, do nothing.
        if (requestCode == LocationUtils.REQUEST_LOCATION &&
                grantResults[0] == 0 &&
                grantResults[1] == 0) {
            LocationUtils.getInstance().resolveLocation(getActivity());

            WeatherManager.getInstance().setZip(LocationUtils.getInstance().getZip());
        }
    }

    private int getScreenWidth() {
        Point size = new Point();
        getActivity().getWindowManager().getDefaultDisplay().getSize(size);
        return size.x;
    }

    private boolean isBetween(int x, int lower, int upper) {
        return lower <= x && x <= upper;
    }
}