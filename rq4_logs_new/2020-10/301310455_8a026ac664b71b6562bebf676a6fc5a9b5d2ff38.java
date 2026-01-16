package com.example.stronginhome;

import androidx.appcompat.app.AppCompatActivity;

import android.content.Intent;
import android.graphics.Color;
import android.os.Bundle;
import android.view.View;
import android.widget.RatingBar;
import android.widget.TextView;
import android.widget.Toast;

public class imc extends AppCompatActivity {
    private TextView bajoPeso, normal,sobrepeso,obesidad,obesidadM,nombre;
    private float peso, estatura, imc;
    private RatingBar ratingBar;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_imc);

        nombre = findViewById(R.id.nombreUsuario);
        bajoPeso = findViewById(R.id.pesoBajo);
        normal = findViewById(R.id.normal);
        sobrepeso = findViewById(R.id.sobrepeso);
        obesidad = findViewById(R.id.obesidad);
        obesidadM = findViewById(R.id.obesidadM);
        peso = formulario.person.getPeso();
        estatura = formulario.person.getEstatura();
        imc =  peso / (estatura * estatura);
        nombre.setText(formulario.person.getNombre());

        ratingBar = (RatingBar)findViewById(R.id.ratingBar);
        ratingBar.setOnRatingBarChangeListener(new RatingBar.OnRatingBarChangeListener() {
            @Override
            public void onRatingChanged(RatingBar ratingBar, float v, boolean b) {
                Toast.makeText(imc.this, "¡GRACIAS POR SU CALIFICACIÓN!", Toast.LENGTH_LONG).show();
            }
        });

        if (imc < 18.5){
            bajoPeso.setBackgroundColor(Color.parseColor("#000000"));
            bajoPeso.setTextColor(Color.parseColor("#FFFFFF"));
        }if (imc >= 18.5 && imc < 25){
            normal.setBackgroundColor(Color.parseColor("#000000"));
            normal.setTextColor(Color.parseColor("#FFFFFF"));
        }if (imc >= 25 && imc < 30){
            sobrepeso.setBackgroundColor(Color.parseColor("#000000"));
            sobrepeso.setTextColor(Color.parseColor("#FFFFFF"));
        }if (imc >= 30 && imc < 35){
            obesidad.setBackgroundColor(Color.parseColor("#000000"));
            obesidad.setTextColor(Color.parseColor("#FFFFFF"));
        }if (imc >= 35 && imc < 40){
            obesidadM.setBackgroundColor(Color.parseColor("#000000"));
            obesidadM.setTextColor(Color.parseColor("#FFFFFF"));
        }


    }
    public void home(View view){
        Intent intent = new Intent (getApplicationContext(), Ejercicios.class);
        startActivity(intent);

    }
}