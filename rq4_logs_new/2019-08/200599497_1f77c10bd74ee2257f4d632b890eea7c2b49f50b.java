package com.irsyaad.dicodinglatihan.latihan3.backstack;

import androidx.appcompat.app.AppCompatActivity;

import android.os.Bundle;
import android.widget.TextView;

import com.irsyaad.dicodinglatihan.latihan3.R;

public class DetailBackstackActivity extends AppCompatActivity {
    TextView tvTitle, tvMessage;
    public static final String EXTRA_TITLE = "extra_title";
    public static final String EXTRA_MESSAGE = "extra_message";

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_detail_backstack);

        tvTitle = findViewById(R.id.tv_title);
        tvMessage = findViewById(R.id.tv_message);

        String title = getIntent().getStringExtra(EXTRA_TITLE);
        String message = getIntent().getStringExtra(EXTRA_MESSAGE);

        tvTitle.setText(title);
        tvMessage.setText(message);

    }
}