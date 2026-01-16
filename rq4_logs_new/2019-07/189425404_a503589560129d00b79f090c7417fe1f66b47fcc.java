package com.squadro.tandir;

import android.app.Activity;

import android.app.AlertDialog;
import android.content.ContentResolver;
import android.content.Context;
import android.content.DialogInterface;
import android.content.Intent;
import android.content.pm.PackageManager;
import android.content.pm.ResolveInfo;
import android.graphics.Color;
import android.graphics.Typeface;
import android.net.Uri;
import android.os.Bundle;

import androidx.appcompat.app.AppCompatActivity;

import android.text.InputType;
import android.util.TypedValue;
import android.view.LayoutInflater;
import android.view.View;

import android.view.ViewGroup;
import android.widget.AdapterView;
import android.widget.ArrayAdapter;
import android.widget.Button;
import android.widget.EditText;
import android.widget.ImageView;
import android.widget.LinearLayout;
import android.widget.ListView;
import android.widget.TextView;
import android.widget.Toast;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import retrofit2.Call;
import retrofit2.Callback;
import retrofit2.Response;
import retrofit2.Retrofit;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;


public class ProfilePageActivity extends AppCompatActivity {

    private Button setButton;
    private String deletionTime;
    static int takeFlags=0;
    private RestAPI rest;
    private RetrofitCreate rc;
    private Retrofit retrofit;
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_profile_page);
        final EditText editTextDeletionName = (EditText) findViewById(R.id.deletionTime);
        editTextDeletionName.setHint("Enter Here...");

        Button setDate = (Button)findViewById(R.id.deletionButton);
        setDate.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                deletionTime = editTextDeletionName.getText().toString();
                //setSettings();
                finish();
                startActivity(getIntent());
                startActivity(new Intent(ProfilePageActivity.this, LandingActivity.class));
            }
        });

    }

    public void setSettings(){
        rc = new RetrofitCreate();
        retrofit = rc.createRetrofit();

        rest = retrofit.create(RestAPI.class);
        JsonObject obj = new JsonObject();

        String nullString = null;
        obj.addProperty("expiredate",deletionTime);
        obj.addProperty("othersettings",nullString);
        obj.addProperty("another",nullString);


/**
        Call<JsonObject> call = rest.setSettings(obj);

        call.enqueue(new Callback<JsonObject>() {
            @Override
            public void onResponse(Call<JsonObject> call, Response<JsonObject> response) {
                Status status = new Status(response.body().get("status").getAsInt(),
                        response.body().get("message").toString());

                Toast.makeText(getBaseContext(),status.getMessage(),Toast.LENGTH_LONG).show();

                //TODO
                //if status is blabla
            }

            @Override
            public void onFailure(Call<JsonObject> call, Throwable t) {
                Toast.makeText(getBaseContext(),"Error!",Toast.LENGTH_LONG).show();
            }
        });**/
    }

}