package com.proyectotec.kevin.proyectotec;

import android.os.Bundle;
import android.support.v7.app.AppCompatActivity;
import android.support.v7.widget.LinearLayoutManager;
import android.support.v7.widget.RecyclerView;

public class VistaModificarLista extends AppCompatActivity {

    RecyclerView recyclerViewModificarLista;
    AdapterProducto adapter;
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_vista_modificar_lista);

        recyclerViewModificarLista = (RecyclerView)findViewById(R.id.RecyclerViewModificarProductos);
        recyclerViewModificarLista.setLayoutManager(new LinearLayoutManager(this));
        adapter = new AdapterProducto(this,ModificarListaActivity.listaTemporal);
        recyclerViewModificarLista.setAdapter(adapter);

    }
}