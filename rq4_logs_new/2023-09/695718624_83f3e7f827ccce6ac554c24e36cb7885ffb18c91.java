package com.example.appveterinaria;

import androidx.annotation.Nullable;
import androidx.appcompat.app.AppCompatActivity;

import android.graphics.Bitmap;
import android.net.Uri;
import android.os.Bundle;
import android.util.Log;
import android.widget.ImageView;
import android.widget.TextView;

import com.android.volley.AuthFailureError;
import com.android.volley.Request;
import com.android.volley.Response;
import com.android.volley.VolleyError;
import com.android.volley.toolbox.ImageRequest;
import com.android.volley.toolbox.JsonArrayRequest;
import com.android.volley.toolbox.JsonObjectRequest;
import com.android.volley.toolbox.Volley;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.HashMap;
import java.util.Map;

public class DetalleMascota extends AppCompatActivity {

    TextView tvNombreMascota, tvTipoanimal, tvRaza, tvColor, tvGenero;
    ImageView ivImgMascota;
    String fotografia;
    int idmascota;
    final String URL = "http://192.168.1.28/appveterinaria/controllers/mascotas.php";
    final String urlImagen = "http://192.168.1.28/appveterinaria/imagenes/";
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_detalle_mascota);
        loadUI();

        Bundle parametros = this.getIntent().getExtras();
        if(parametros != null){
            idmascota = parametros.getInt("idmascota");
        }
        obtenerDatos(idmascota);
    }
    private void loadUI(){
        tvNombreMascota =  findViewById(R.id.tvNombreMascota);
        tvTipoanimal =  findViewById(R.id.tvTipoanimal);
        tvRaza =  findViewById(R.id.tvRaza);
        tvColor =  findViewById(R.id.tvColor);
        tvColor =  findViewById(R.id.tvGenero);

        ivImgMascota =  findViewById(R.id.ivImgMascota);
    }


    private void obtenerDatos(int idmascota){
        Uri.Builder NEWURL = Uri.parse(URL).buildUpon();
        NEWURL.appendQueryParameter("operacion", "buscar");
        NEWURL.appendQueryParameter("idmascota", String.valueOf(idmascota));
        String urlnueva = NEWURL.build().toString();
        JsonObjectRequest jsonObjectRequest = new JsonObjectRequest(Request.Method.GET, urlnueva, null, new Response.Listener<JSONObject>() {
            @Override
            public void onResponse(JSONObject response) {
                try {
                    Log.d("datos", response.getString("fotografia"));
                    tvNombreMascota.setText(response.getString("nombre"));
                    tvTipoanimal.setText(response.getString("nombreamimal"));
                    tvRaza.setText(response.getString("nombreRaza"));
                    tvColor.setText(response.getString("color"));
                    tvGenero.setText(response.getString("genero"));
                    obtenerImagen(response.getString("fotografia"));
                }catch (Exception e){
                    e.printStackTrace();
                }

            }
        }, new Response.ErrorListener() {
            @Override
            public void onErrorResponse(VolleyError error) {
                Log.e("Error Volley", error.toString());
            }
        });
        Volley.newRequestQueue(this).add(jsonObjectRequest);

    }
    private void obtenerImagen(String foto){
        String urlNueva = urlImagen+foto;
        Log.e("Fotoooooo", urlNueva);
        ImageRequest imageRequest = new ImageRequest(urlNueva, new Response.Listener<Bitmap>() {
            @Override
            public void onResponse(Bitmap response) {
                ivImgMascota.setImageBitmap(response);
            }
        }, 0, 0, null, null, new Response.ErrorListener() {
            @Override
            public void onErrorResponse(VolleyError error) {
                Log.e("Error imagen", error.toString());
            }
        });
        Volley.newRequestQueue(this).add(imageRequest);
    }


}