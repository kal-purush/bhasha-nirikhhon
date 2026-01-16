package com.fiuba.tdpii.correapp.activities;

import android.content.Intent;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.view.View;
import android.widget.Button;
import android.widget.ImageView;
import android.widget.RatingBar;
import android.widget.Switch;
import android.widget.Toast;

import com.fiuba.tdpii.correapp.R;

public class RateTripDriverActivity extends AppCompatActivity {


    public final static String TRIP_ID_KEY = "trip_id_key";

    private Switch switchMaterialDriver;
    private Switch switchMaterialApp;
    private Switch switchMaterialCar;
    private Button buttonSubmit;
    private RatingBar ratingBar;

    private int tripId;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_rate_trip);

        Intent intent = getIntent();
        tripId = intent.getIntExtra(TRIP_ID_KEY, 0);

        if (tripId == 0) {
//            showErrorMessage();
            // finish();
            // return;
        }

        ratingBar = (RatingBar) findViewById(R.id.rating_bar);
        switchMaterialDriver = findViewById(R.id.switch_material_driver);
        switchMaterialApp =  findViewById(R.id.switch_material_app);
        switchMaterialCar =  findViewById(R.id.switch_material_car);

        buttonSubmit = findViewById(R.id.button_submit);
        buttonSubmit.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                // TODO: implementar puntuado de viaje para cliente. Falta servicio de API.
                int rating = ratingBar.getNumStars();
                boolean improveDriver = switchMaterialDriver.isChecked();
                boolean improveApp= switchMaterialApp.isChecked();
                boolean improveCar = switchMaterialCar.isChecked();

                Intent mainActivity = new Intent(getApplicationContext(), MainActivity.class);
                startActivity(mainActivity);
            }
        });
    }

    @Override
    protected void onSaveInstanceState(Bundle savedInstanceState) {
        savedInstanceState.putInt(TRIP_ID_KEY, tripId);

        super.onSaveInstanceState(savedInstanceState);
    }

    @Override
    protected void onRestoreInstanceState(Bundle savedInstanceState) {
        super.onRestoreInstanceState(savedInstanceState);

        tripId = savedInstanceState.getInt(TRIP_ID_KEY);
    }

    private void showErrorMessage() {
        Toast.makeText(this, "Se produjo un error inesperado, intente nuevamente", Toast.LENGTH_LONG).show();
    }
}