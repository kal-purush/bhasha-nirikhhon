package com.fiuba.tdpii.correapp.activities;

import android.content.Intent;
import android.os.Bundle;
import android.view.View;
import android.widget.CompoundButton;
import android.widget.ImageView;
import android.widget.LinearLayout;
import android.widget.RatingBar;
import android.widget.Toast;

import com.fiuba.tdpii.correapp.R;
import com.google.android.material.button.MaterialButton;
import com.google.android.material.switchmaterial.SwitchMaterial;
import com.google.android.material.textfield.TextInputEditText;

import androidx.appcompat.app.AppCompatActivity;

public class RateTripClientActivity extends AppCompatActivity {

    public final static String TRIP_ID_KEY = "trip_id_key";

    private SwitchMaterial switchMaterialDriver;
    private SwitchMaterial switchMaterialApp;
    private SwitchMaterial switchMaterialCar;
    private ImageView backArrow;
    private MaterialButton buttonSubmit;
    private RatingBar ratingBar;

    private int tripId;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_rate_trip_client);

        Intent intent = getIntent();
        tripId = intent.getIntExtra(TRIP_ID_KEY, 0);

        if (tripId == 0) {
            showErrorMessage();
            // finish();
            // return;
        }

        ratingBar = (RatingBar) findViewById(R.id.rating_bar);
        backArrow = (ImageView) findViewById(R.id.back_arrow);
        backArrow.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                RateTripClientActivity.super.onBackPressed();
            }
        });
        switchMaterialDriver = (SwitchMaterial) findViewById(R.id.switch_material_driver);
        switchMaterialApp = (SwitchMaterial) findViewById(R.id.switch_material_app);
        switchMaterialCar = (SwitchMaterial) findViewById(R.id.switch_material_car);

        buttonSubmit = (MaterialButton) findViewById(R.id.button_submit);
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