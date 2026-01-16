package com.example.junior.volleyapp;

import android.content.Intent;
import android.os.Bundle;
import android.support.v7.app.ActionBarActivity;
import android.widget.TextView;


public class Detalhes extends ActionBarActivity {
    private TextView data;
    private TextView nnota;
    private TextView produto;
    private TextView desc;
    private Intent intent;
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_detalhes);
        intent = getIntent();

        data    = (TextView) findViewById(R.id.detalhes_txt_data);
        nnota   = (TextView) findViewById(R.id.detalhes_txt_nnotas);
        produto = (TextView) findViewById(R.id.detalhes_txt_produtos);
        desc    = (TextView) findViewById(R.id.detalhes_txt_desc);

        data.setText(intent.getStringExtra("data"));
        nnota.setText(intent.getStringExtra("nnota"));
        produto.setText(intent.getStringExtra("produto"));
        desc.setText(intent.getStringExtra("desc"));

    }
}