package com.fiuba.tdpii.correapp.activities;

import androidx.appcompat.app.AppCompatActivity;

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

public class RateTripDriverActivity extends AppCompatActivity {

    public final static String TRIP_ID_KEY = "trip_id_key";

    private SwitchMaterial switchMaterial;
    private LinearLayout commentLayout;
    private ImageView backArrow;
    private MaterialButton buttonSubmit;
    private RatingBar ratingBar;
    private TextInputEditText textInputComment;

    private int tripId;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_rate_trip_driver);

        Intent intent = getIntent();
        tripId = intent.getIntExtra(TRIP_ID_KEY, 0);

        if (tripId == 0) {
            showErrorMessage();
            // finish();
            // return;
        }

        ratingBar = (RatingBar) findViewById(R.id.rating_bar);
        commentLayout = (LinearLayout) findViewById(R.id.comment_layout);
        textInputComment = (TextInputEditText) findViewById(R.id.comment);
        backArrow = (ImageView) findViewById(R.id.back_arrow);
        backArrow.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                RateTripDriverActivity.super.onBackPressed();
            }
        });
        switchMaterial = (SwitchMaterial) findViewById(R.id.switch_material);
        switchMaterial.setOnCheckedChangeListener(new CompoundButton.OnCheckedChangeListener() {
            public void onCheckedChanged(CompoundButton buttonView, boolean isChecked) {
                if (isChecked) {
                    commentLayout.setVisibility(View.VISIBLE);
                } else {
                    commentLayout.setVisibility(View.INVISIBLE);
                }
            }
        });
        buttonSubmit = (MaterialButton) findViewById(R.id.button_submit);
        buttonSubmit.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                // TODO: implementar puntuado de viaje para chofer. Falta servicio de API.
                int rating = ratingBar.getNumStars();
                boolean isClientGood = !switchMaterial.isChecked();
                String comment = textInputComment.getText().toString();

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