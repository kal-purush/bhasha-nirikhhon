package com.fiuba.tdpii.correapp.activities;

import android.support.design.widget.TabItem;
import android.support.design.widget.TabLayout;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.view.View;
import android.widget.Button;
import android.widget.TextView;

import com.fiuba.tdpii.correapp.R;

import java.util.Date;

public class CreateTripActivity extends AppCompatActivity {


    private static final int NOW_TAB_POSITION = 0;
    private static final int LATER_TAB_POSITION = 1;

    private TabItem viajarAhora;
    private TabItem reservar;
    private TabLayout tabs;

    private TextView origen;
    private TextView destino;
    private TextView mascotas;

    private TextView fecha;
    private TextView hora;


    private Button reservarBtn;

    private Boolean now = Boolean.TRUE;

    private Date tripDate;
    private

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_create_trip);


        viajarAhora = findViewById(R.id.trip_now);

        reservar = findViewById(R.id.reservar);
        tabs = findViewById(R.id.tabLayout);


        origen = findViewById(R.id.origen);
        destino = findViewById(R.id.destino);
        mascotas = findViewById(R.id.mascotas);
        fecha = findViewById(R.id.fecha);
        hora = findViewById(R.id.hora);
        reservarBtn = findViewById(R.id.confirm);

        setUpEvents();
    }

    private void setUpEvents(){

//        viajarAhora.setZ(viajarAhora.getTranslationZ() + 100f);
//        viajarAhora.setTextColor(Color.WHITE);
//        viajarAhora.setOnClickListener(new View.OnClickListener() {
//            @Override
//            public void onClick(View v) {
//
//                if(paraReservar) {
//                    viajarAhora.setHighlightColor(Color.WHITE);
//                    reservar.setHighlightColor(R.color.black_overlay);
//
//                    viajarAhora.setZ(viajarAhora.getTranslationZ() + 100f);
//                    paraReservar = !paraReservar;
//                }
//
//            }
//        });
//        reservar.setOnClickListener(new View.OnClickListener() {
//            @Override
//            public void onClick(View v) {
//
//                if(!paraReservar) {
//                    reservar.setHighlightColor(Color.WHITE);
//                    viajarAhora.setHighlightColor(R.color.black_overlay);
//
//                    reservar.setZ(viajarAhora.getTranslationZ() + 100f);
//                    paraReservar = !paraReservar;
//                }
//
//            }
//        });

        tabs.setOnTabSelectedListener(new TabLayout.OnTabSelectedListener() {
            @Override
            public void onTabSelected(TabLayout.Tab tab) {
                int position = tab.getPosition();

                switch (position){
                    case NOW_TAB_POSITION:

                        fecha.setVisibility(View.GONE);
                        hora.setVisibility(View.GONE);

                        now = Boolean.TRUE;

                        break;
                    case LATER_TAB_POSITION:
                        fecha.setVisibility(View.VISIBLE);
                        hora.setVisibility(View.VISIBLE);

                        now = Boolean.FALSE;

                        break;
                }

            }

            @Override
            public void onTabUnselected(TabLayout.Tab tab) {

            }

            @Override
            public void onTabReselected(TabLayout.Tab tab) {

            }
        });


    }
}