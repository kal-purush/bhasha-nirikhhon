package com.example.enigmator.activity;

import android.app.Activity;
import android.content.Intent;
import android.os.Bundle;
import android.support.v7.app.AppCompatActivity;
import android.view.View;
import android.widget.Button;
import android.widget.LinearLayout;
import android.widget.ProgressBar;

import com.example.enigmator.R;
import com.example.enigmator.controller.HttpManager;
import com.example.enigmator.controller.HttpRequest;
import com.example.enigmator.entity.Enigma;
import com.example.enigmator.entity.Response;
import com.example.enigmator.entity.UserEnigmator;
import com.google.gson.Gson;

public class EnigmaActivity extends AppCompatActivity {
    public static final String ENIGMA_ID_KEY = "enigma_id_key";
    public static final String ENIGMA_KEY = "enigma_key";
    public static final String VALIDATION_STATUS_KEY = "validation_status_key";

    private ProgressBar progressLoading;
    private HttpManager httpManager;
    private Gson gson;

    private Enigma enigma;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_enigma);

        httpManager = new HttpManager(this);
        gson = new Gson();
        progressLoading = findViewById(R.id.progress_loading);

        Intent intent = getIntent();
        String enigmaJson = intent.getStringExtra(ENIGMA_KEY);
        if (enigmaJson != null) {
            enigma = gson.fromJson(enigmaJson, Enigma.class);
            setupScreen();
        } else {
            int id = intent.getIntExtra(ENIGMA_ID_KEY, -1);
            if (id < 0) {
                finish();
            } else {
                httpManager.addToQueue(HttpRequest.GET, "/Enigmes/" + id, null, new HttpRequest.HttpRequestListener() {
                    @Override
                    public void prepareRequest() {
                        progressLoading.setVisibility(View.VISIBLE);
                    }

                    @Override
                    public void handleSuccess(Response response) {
                        progressLoading.setVisibility(View.GONE);

                        enigma = gson.fromJson(response.getContent(), Enigma.class);
                        setupScreen();
                    }

                    @Override
                    public void handleError(Response error) {
                        progressLoading.setVisibility(View.GONE);
                        finish();
                    }
                });
            }
        }
    }

    private void setupScreen() {
        setTitle(enigma.getName());

        // TODO: other setup


        // Validator setup
        boolean isValidator = UserEnigmator.getCurrentUser(this).isValidator();
        if (isValidator) {
            LinearLayout layoutButtons = findViewById(R.id.layout_validator_buttons);
            layoutButtons.setVisibility(View.VISIBLE);

            Button btnReject = findViewById(R.id.btn_reject);
            Button btnValidate = findViewById(R.id.btn_validate);

            final Intent intent = new Intent();
            intent.putExtra(ENIGMA_ID_KEY, enigma.getId());

            btnReject.setOnClickListener(new View.OnClickListener() {
                @Override
                public void onClick(View v) {
                    // TODO: POST Rejection


                    intent.putExtra(VALIDATION_STATUS_KEY, false);
                    setResult(Activity.RESULT_OK, intent);
                    finish();
                }
            });

            btnValidate.setOnClickListener(new View.OnClickListener() {
                @Override
                public void onClick(View v) {
                    httpManager.addToQueue(HttpRequest.PATCH,
                            "/Enigmes/" + enigma.getId() + "/ValidateEnigme", null,null);

                    intent.putExtra(VALIDATION_STATUS_KEY, true);
                    setResult(Activity.RESULT_OK, intent);
                    finish();
                }
            });
        }
    }

    @Override
    public void onBackPressed() {
        setResult(Activity.RESULT_CANCELED);
        super.onBackPressed();
    }
}