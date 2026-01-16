package com.example.enigmator.activity;

import android.content.Context;
import android.content.SharedPreferences;
import android.preference.PreferenceManager;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.view.View;
import android.widget.Button;
import android.widget.CheckBox;
import android.widget.TextView;
import android.widget.Toast;

import com.example.enigmator.R;

public class TermsActivity extends AppCompatActivity {
    private static final String GTC_PREF = "gtc_pref";

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_terms);

        final CheckBox checkBoxAgree = findViewById(R.id.checkbox_agree);
        Button btnAgree = findViewById(R.id.btn_agree);
        TextView textAgree = findViewById(R.id.text_agree);
        textAgree.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                checkBoxAgree.setChecked(!checkBoxAgree.isChecked());
            }
        });

        btnAgree.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                if (!checkBoxAgree.isChecked()) {
                    Toast.makeText(TermsActivity.this, R.string.toast_must_confirm, Toast.LENGTH_SHORT).show();
                } else {
                    SharedPreferences preferences = PreferenceManager.getDefaultSharedPreferences(TermsActivity.this);
                    preferences.edit()
                            .putBoolean(GTC_PREF, true)
                            .apply();
                    finish();
                }
            }
        });
    }

    @Override
    public void onBackPressed() {
        // Avoid return to previous activity
    }

    static boolean hasAgreed(Context context) {
        SharedPreferences preferences = PreferenceManager.getDefaultSharedPreferences(context);
        return preferences.getBoolean(GTC_PREF, false);
    }
}