package com.example.laurenfalzarano.shots.Activities;

import android.content.Intent;
import android.os.Bundle;
import android.support.design.widget.Snackbar;
import android.support.v7.app.AppCompatActivity;
import android.view.Menu;
import android.view.MenuItem;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;

import com.example.laurenfalzarano.shots.Persistence.PreferencesManager;
import com.example.laurenfalzarano.shots.R;
import com.example.laurenfalzarano.shots.Utils.FormUtils;

import butterknife.Bind;
import butterknife.ButterKnife;
import butterknife.OnClick;

public class ShotCounterFormActivity extends AppCompatActivity {

    public static final String SHOT_COUNTER_NAME_KEY = "shotCounterName";

    @Bind(R.id.counter_name) EditText counterName;
    @Bind(R.id.shots_count) EditText shotsCount;
    @Bind(R.id.submit_counter) Button submitCounter;
    @Bind(R.id.parent) View parent;

    private boolean editMode = false;
    private String oldCounterName;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.shot_counter_form);
        ButterKnife.bind(this);
        getSupportActionBar().setDisplayHomeAsUpEnabled(true);

        Intent intent = getIntent();
        oldCounterName = intent.getStringExtra(SHOT_COUNTER_NAME_KEY);

        if (oldCounterName != null) {
            editMode = true;
            counterName.setText(oldCounterName);
            String numShots = String.valueOf(PreferencesManager.get().getNumShotsForCounter(oldCounterName));
            shotsCount.setText(numShots);
            submitCounter.setText(getString(R.string.update));
        }
    }

    @OnClick(R.id.submit_counter)
    public void submitCounter(View view) {
        FormUtils.hideKeyboard(this);
        String shotCounterName = counterName.getText().toString();
        if (shotCounterName.trim().isEmpty()) {
            Snackbar.make(parent, R.string.no_blank_names, Snackbar.LENGTH_LONG).show();
        }
        else if (PreferencesManager.get().isCounterNameTaken(shotCounterName)) {
            Snackbar.make(parent, R.string.no_duplicate_counters, Snackbar.LENGTH_LONG).show();
        }
        else {
            int numShots = Integer.parseInt(shotsCount.getText().toString());
            if (editMode) {
                PreferencesManager.get().changeCounterName(oldCounterName, shotCounterName);
            }
            else {
                PreferencesManager.get().addShotCounter(shotCounterName);
            }
            PreferencesManager.get().setShotsCount(shotCounterName, numShots);
            finish();
        }
    }

    @OnClick(R.id.count_decrement)
    public void decrement(View view) {
        int count = Integer.parseInt(shotsCount.getText().toString());
        if (count != 0) {
            String newCount = String.valueOf(count - 1);
            shotsCount.setText(newCount);
        }
    }

    @OnClick(R.id.count_increment)
    public void increment(View view) {
        int count = Integer.parseInt(shotsCount.getText().toString());
        if (count != 999) {
            String newCount = String.valueOf(count + 1);
            shotsCount.setText(newCount);
        }
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        getMenuInflater().inflate(R.menu.blank_menu, menu);
        return true;
    }

    @Override
    public boolean onOptionsItemSelected(MenuItem item) {
        if (item.getItemId() == android.R.id.home) {
            FormUtils.hideKeyboard(this);
            finish();
            return true;
        }
        return super.onOptionsItemSelected(item);
    }

}