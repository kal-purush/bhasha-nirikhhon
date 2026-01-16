package com.studio.azhar.examplecrudvolley;

import android.content.Intent;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.support.v7.widget.RecyclerView;
import android.util.Log;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;
import android.widget.Toast;

import com.android.volley.AuthFailureError;
import com.android.volley.Request;
import com.android.volley.RequestQueue;
import com.android.volley.Response;
import com.android.volley.VolleyError;
import com.android.volley.toolbox.StringRequest;
import com.android.volley.toolbox.Volley;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UpdateDeleteActivity extends AppCompatActivity {

    private static final String URL_MHS = "http://192.168.167.1/MateriCRUD/getDataMhs.php?id=";
    private static final String URL_UPDATE = "http://192.168.167.1/MateriCRUD/updateMhs.php";
    private static final String URL_DELETE = "http://192.168.167.1/MateriCRUD/deleteMhs.php";

    TextView ID;
    EditText NPM, NAMA;
    Button btnDelete, btnUpdate;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_update_delete);

        ID = findViewById(R.id.tvID);
        NPM = findViewById(R.id.tvNPM);
        NAMA = findViewById(R.id.tvNama);

        btnDelete = findViewById(R.id.btnDelete);
        btnUpdate = findViewById(R.id.btnUpdate);

        btnUpdate.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                updateMhs();
            }
        });

        btnDelete.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                deleteMhs();
            }
        });

        getData();
    }

    public void getData(){
        Intent intent = getIntent();
        final String mId = intent.getStringExtra(MahasiswaAdapter.KEY_ID);
        String url = URL_MHS + java.net.URLEncoder.encode(mId);
        StringRequest stringRequest = new StringRequest(Request.Method.GET,
                url, new Response.Listener<String>() {
            @Override
            public void onResponse(String response) {
                try {
                    JSONObject jsonObject = new JSONObject(response);

                    JSONArray array = jsonObject.getJSONArray("results");

                    String mId = array.getJSONObject(0).getString("id");
                    String nNpm = array.getJSONObject(0).getString("npm");
                    String nNama = array.getJSONObject(0).getString("nama");

                    ID.setText(mId);
                    NPM.setText(nNpm);
                    NAMA.setText(nNama);

                } catch (JSONException ex) {
                    Log.d("updateMHS", ex.getMessage());
                }

            }

        }, new Response.ErrorListener() {
            @Override
            public void onErrorResponse(VolleyError error) {
                //lakukan sesuatu jika error
            }
        });

        RequestQueue requestQueue = Volley.newRequestQueue(this);
        requestQueue.add(stringRequest);
    }

    public void updateMhs (){
        final String id = ID.getText().toString().trim();
        final String npm = NPM.getText().toString().trim();
        final String nama = NAMA.getText().toString().trim();

        StringRequest stringRequest1 = new StringRequest(Request.Method.POST, URL_UPDATE,
                new Response.Listener<String>() {
                    @Override
                    public void onResponse(String response) {
                        Toast.makeText(UpdateDeleteActivity.this, response, Toast.LENGTH_SHORT).show();
                    }
                },
                new Response.ErrorListener() {
                    @Override
                    public void onErrorResponse(VolleyError error) {
                        Toast.makeText(UpdateDeleteActivity.this, error.toString(), Toast.LENGTH_LONG).show();
                    }
                }) {
            @Override
            protected Map<String, String> getParams() {
                Map<String, String> params = new HashMap<String, String>();

                params.put("id", id);
                params.put("npm", npm);
                params.put("nama", nama);

                return params;
            }
        };
        RequestQueue requestQueue1 = Volley.newRequestQueue(this);
        requestQueue1.add(stringRequest1);
    }

    public void deleteMhs(){
        final String id = ID.getText().toString().trim();


        StringRequest stringRequest2 = new StringRequest(Request.Method.POST, URL_DELETE,
                new Response.Listener<String>() {
                    @Override
                    public void onResponse(String response) {
                        Toast.makeText(UpdateDeleteActivity.this, response, Toast.LENGTH_SHORT).show();
                    }
                },
                new Response.ErrorListener() {
                    @Override
                    public void onErrorResponse(VolleyError error) {
                        Toast.makeText(UpdateDeleteActivity.this, error.toString(), Toast.LENGTH_LONG).show();
                    }
                }) {
            @Override
            protected Map<String, String> getParams() {
                Map<String, String> params = new HashMap<String, String>();

                params.put("id", id);

                return params;
            }
        };
        RequestQueue requestQueue2 = Volley.newRequestQueue(this);
        requestQueue2.add(stringRequest2);
    }
}