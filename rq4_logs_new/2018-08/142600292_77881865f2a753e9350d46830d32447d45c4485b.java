package com.proyectotec.kevin.proyectotec;

import android.os.Bundle;
import android.support.v7.app.AppCompatActivity;
import android.support.v7.widget.LinearLayoutManager;
import android.support.v7.widget.RecyclerView;

public class ProductosEnCarretaActivity extends AppCompatActivity {

    RecyclerView recyclerViewProductosEnCarreta;
    AdapterProducto adapter;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_productos_en_carreta);
        getSupportActionBar().setDisplayShowHomeEnabled(true);
        getSupportActionBar().setDisplayHomeAsUpEnabled(true);

        recyclerViewProductosEnCarreta = (RecyclerView)findViewById(R.id.RecyclerViewListaProductos);
        recyclerViewProductosEnCarreta.setLayoutManager(new LinearLayoutManager(this));
        adapter = new AdapterProducto(this,ListaActivity.listaTemporalCarreta);
        recyclerViewProductosEnCarreta.setAdapter(adapter);


    }
}