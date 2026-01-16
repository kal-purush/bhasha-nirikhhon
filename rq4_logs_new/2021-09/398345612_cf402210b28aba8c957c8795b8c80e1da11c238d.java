package in.koshurtech.zstmx.activities;

import androidx.appcompat.app.AppCompatActivity;
import androidx.appcompat.app.AppCompatDelegate;
import androidx.preference.PreferenceManager;
import androidx.recyclerview.widget.LinearLayoutManager;
import androidx.recyclerview.widget.RecyclerView;

import android.content.Intent;
import android.content.SharedPreferences;
import android.graphics.Color;
import android.graphics.drawable.ColorDrawable;
import android.os.Bundle;
import android.view.View;
import android.widget.ScrollView;
import android.widget.TextView;
import android.widget.Toast;

import com.android.volley.AuthFailureError;
import com.android.volley.DefaultRetryPolicy;
import com.android.volley.NetworkResponse;
import com.android.volley.Request;
import com.android.volley.RequestQueue;
import com.android.volley.Response;
import com.android.volley.ServerError;
import com.android.volley.VolleyError;
import com.android.volley.toolbox.HttpHeaderParser;
import com.android.volley.toolbox.JsonObjectRequest;
import com.android.volley.toolbox.Volley;
import com.google.android.material.button.MaterialButton;
import com.google.android.material.floatingactionbutton.ExtendedFloatingActionButton;
import com.google.android.material.progressindicator.LinearProgressIndicator;
import com.google.gson.Gson;

import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import in.koshurtech.zstmx.R;
import in.koshurtech.zstmx.adapters.recyclerInfoViewAdapter;
import in.koshurtech.zstmx.javaClasses.recyclerInfoView;

public class profileShowcase extends AppCompatActivity {

    JSONObject jsonObject;
    LinearProgressIndicator linearProgressIndicator;
    RecyclerView recyclerView;
    recyclerInfoViewAdapter recyclerInfoViewAdapter;
    ArrayList<recyclerInfoView> recyclerInfoViewArrayList = new ArrayList<>();
    ExtendedFloatingActionButton extendedFloatingActionButton;
    ScrollView scrollView;
    TextView deviceMake2;
    TextView deviceModel2;
    MaterialButton upVoteButton;
    MaterialButton downVoteButton;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);

        SharedPreferences sp  = PreferenceManager.getDefaultSharedPreferences(this);
        String theme = sp.getString("appTheme","0");
        if(theme.equals("0")){
            AppCompatDelegate.setDefaultNightMode(AppCompatDelegate.MODE_NIGHT_FOLLOW_SYSTEM);
        }
        else if(theme.equals("1")){
            setTheme(R.style.Theme_ZstmX_day);
            getSupportActionBar().setBackgroundDrawable(new ColorDrawable(Color.parseColor("#ffffff")));
            getSupportActionBar().setElevation(0);
        }
        else if(theme.equals("2")){
            setTheme(R.style.Theme_ZstmX_night);
            getSupportActionBar().setBackgroundDrawable(new ColorDrawable(Color.parseColor("#121212")));

        }
        setContentView(R.layout.activity_profile_showcase);

        getSupportActionBar().setElevation(0);

        deviceMake2 = (TextView) findViewById(R.id.deviceModelShow);
        deviceModel2 = (TextView) findViewById(R.id.deviceMakeShow);
        recyclerView = (RecyclerView) findViewById(R.id.deviceParametersRecyclerView);
        linearProgressIndicator = (LinearProgressIndicator) findViewById(R.id.showProfileLPI);
        linearProgressIndicator.setVisibility(View.VISIBLE);
        extendedFloatingActionButton = (ExtendedFloatingActionButton) findViewById(R.id.goUp);
        scrollView = (ScrollView) findViewById(R.id.scrollViewbelowlpi);
        upVoteButton = (MaterialButton) findViewById(R.id.upVoteButton2);
        downVoteButton = (MaterialButton) findViewById(R.id.downVoteButton2);


        upVoteButton.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {

            }
        });

        downVoteButton.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {

            }
        });


        LinearLayoutManager layoutManager = new LinearLayoutManager(getApplicationContext());
        recyclerView.setLayoutManager(layoutManager);

        recyclerInfoViewAdapter = new recyclerInfoViewAdapter(recyclerInfoViewArrayList,getApplicationContext(),profileShowcase.this);



        scrollView.setSmoothScrollingEnabled(true);
        scrollView.setVisibility(View.INVISIBLE);

        extendedFloatingActionButton.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {

            }
        });

        Intent intent = getIntent();
        String id = intent.getStringExtra("id");
        getProfileData(id);




    }



    boolean isLoading = false;

    private void getProfileData(String id){
        isLoading = true;
        linearProgressIndicator.setVisibility(View.VISIBLE);
        recyclerInfoViewArrayList.clear();

        HashMap<String,String> headers = new HashMap<>();
        headers.put("Content-Type","application/json");

        HashMap<String,String> payload = new HashMap<>();
        payload.put("id",id);

        JsonObjectRequest jsonObjectRequest =
                new JsonObjectRequest(Request.Method.POST,
                        getString(R.string.getProfileByIdFormServer),
                        new JSONObject(payload),
                        new Response.Listener<JSONObject>() {
                            @Override
                            public void onResponse(JSONObject response) {
                                isLoading = false;

                                linearProgressIndicator.setVisibility(View.INVISIBLE);
                                try {
                                    jsonObject = response;
                                    deviceMake2.setText(response.getString("deviceMake"));
                                    deviceModel2.setText(response.getString("deviceModel"));

                                    JSONArray jsonArray = response.getJSONArray("deviceInfo");


                                    recyclerView.setAdapter(recyclerInfoViewAdapter);



                                    for(int i=0; i<jsonArray.length(); i++){

                                        JSONObject jsonObject  = jsonArray.getJSONObject(i);
                                        JSONArray jsonArray2 = jsonObject.getJSONArray("data");
                                        AtomicReference<String> ssrr = new AtomicReference<>("");
                                        for(int j=0; j<jsonArray2.length(); j++){
                                            JSONObject JSONObject2  = jsonArray2.getJSONObject(j);
                                            HashMap<String, Object> yourHashMap = new Gson().fromJson(JSONObject2.toString(), HashMap.class);
                                            yourHashMap.entrySet().forEach(entry -> {
                                                ssrr.set(ssrr + entry.getKey() + " " + entry.getValue()+"\n");
                                            });

                                        }

                                        recyclerInfoViewArrayList.add(new recyclerInfoView(ssrr.toString(),jsonObject.getString("pro")));
                                    }

                                    recyclerInfoViewAdapter.updateList(recyclerInfoViewArrayList);



                                    scrollView.setVisibility(View.VISIBLE);
                                }
                                catch (Exception e){
                                    e.printStackTrace();
                                }
                            }
                        }, new Response.ErrorListener() {



                    @Override
                    public void onErrorResponse(VolleyError error) {
                        isLoading = false;
                        linearProgressIndicator.setVisibility(View.INVISIBLE);


                        NetworkResponse networkResponse = error.networkResponse;
                        if(error instanceof ServerError && networkResponse!=null){
                            try{
                                String res = new String(networkResponse.data, HttpHeaderParser.parseCharset(networkResponse.headers,  "utf-8"));
                                Toast.makeText(getApplicationContext(),res,Toast.LENGTH_LONG).show();
                            }
                            catch (Exception err){
                                err.printStackTrace();

                            }
                        }
                    }
                }){
                    @Override
                    public Map<String, String> getHeaders() throws AuthFailureError {
                        return headers;
                    }
                };

        RequestQueue requestQueue = Volley.newRequestQueue(getApplicationContext());
        jsonObjectRequest.setRetryPolicy(new DefaultRetryPolicy(
                0,
                DefaultRetryPolicy.DEFAULT_MAX_RETRIES,
                DefaultRetryPolicy.DEFAULT_BACKOFF_MULT));
        requestQueue.add(jsonObjectRequest);


    }
}