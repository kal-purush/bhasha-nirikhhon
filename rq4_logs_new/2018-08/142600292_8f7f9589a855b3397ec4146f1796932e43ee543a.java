package com.proyectotec.kevin.proyectotec;

import android.app.Activity;
import android.content.Intent;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.view.View;
import android.widget.TextView;

public class BuscarListaActivity extends AppCompatActivity {

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_buscar_lista);

        //Lleva al activity ListaOrdenada
        TextView opcionOrdenar =(TextView)findViewById(R.id.ordenarLista);
        //Asigna un clicklistener al activity Lista Ordenada
        opcionOrdenar.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                //Crea un intent para abrir {@link ListaOrdenadaActivity}
                Intent ordenarIntent = new Intent(BuscarListaActivity.this, ListaOrdenadaActivity.class );
                //Inicia el activity ListaOrdenada
                startActivity(ordenarIntent);
            }
        });


    }
}