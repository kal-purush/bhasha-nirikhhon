package com.example.yesii.jujuyeventos;

import android.content.Intent;
import android.provider.Settings;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.view.View;
import android.widget.ImageView;
import android.widget.MediaController;
import android.widget.TextView;
import android.widget.VideoView;

public class DetalleActivity extends AppCompatActivity {
    TextView txtTitulo,txtLugar1,txtFecha,txtHora1,txtDetalle,txtPrecio1,txtLugar2,txtHora2,txtPrecio2;
    ImageView imageView;
    VideoView videoView;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_detalle);

        // Get the Intent that started this activity and extract the string
        Intent intent = getIntent();
        String titulo = intent.getStringExtra("titulo");
        String lugar1 = intent.getStringExtra("lugar1");
        String lugar2 = intent.getStringExtra("lugar2");
        String fecha = intent.getStringExtra("fecha");
        String hora1 = intent.getStringExtra("hora1");
        String hora2 = intent.getStringExtra("hora2");
        String detalle = intent.getStringExtra("detalle");
        String precio1 = intent.getStringExtra("precio1");
        String precio2 = intent.getStringExtra("precio2");
        String categoria = intent.getStringExtra("categoria");
        String videoView = intent.getStringExtra("videoView");
////            if("cine".equals(evento.getCategoria())) {
////                View view1=inflater.inflate(R.layout.fragment_detallecine, container, false);
////                view=view1;
////                videoView = (VideoView) findViewById(R.id.videoView);
////                txtLugar2 =(TextView) findViewById(R.id.lugar2Txt);
////                txtHora2 =(TextView) findViewById(R.id.hora2Txt);
////                txtPrecio2 =(TextView) findViewById(R.id.precio2Txt);
////                txtLugar2.setText("LUGAR: " + evento.getLugar1());
////                txtHora2.setText("HORA: " + evento.getHora1());
////                txtPrecio2.setText("PRECIO: " + evento.getPrecio1());
////                videoView.setVideoURI(evento.getVideoView());
////                videoView.setMediaController(new MediaController(getContext()));
////                videoView.start();
////                videoView.requestFocus();
////            }else {
//                imageView = (ImageView) findViewById(R.id.imageViewImg);
//                imageView.setImageDrawable(evento.getImage());
////            }
            txtTitulo =(TextView) findViewById(R.id.tituloTxt);
            txtLugar1 =(TextView) findViewById(R.id.lugar1Txt);
            txtFecha =(TextView) findViewById(R.id.fechaTxt);
            txtHora1 =(TextView) findViewById(R.id.hora1Txt);
            txtDetalle =(TextView) findViewById(R.id.detalleTxt);
            txtPrecio1 =(TextView) findViewById(R.id.precio1Txt);
            txtTitulo.setText(titulo);
            txtLugar1.setText("LUGAR: " + lugar1);
//            txtFecha.setText("FECHA: " + fecha);
            txtHora1.setText("HORA: " + hora1);
            txtDetalle.setText(detalle);
            txtPrecio1.setText("PRECIO: " + precio1);
    }
}