package com.ineedyourcode.calculator.theme;

import androidx.appcompat.app.AppCompatActivity;

import android.app.Activity;
import android.content.Intent;
import android.os.Bundle;
import android.view.LayoutInflater;
import android.view.View;
import android.widget.ImageView;
import android.widget.LinearLayout;
import android.widget.TextView;
import android.widget.Toast;

import com.ineedyourcode.calculator.R;

public class SelectThemeActivity extends AppCompatActivity {

    public static final String EXTRA_THEME = "EXTRA_THEME";

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_select_theme);

        LinearLayout themeContainer = findViewById(R.id.themes);

        Intent launchIntent = getIntent();

        if (launchIntent.hasExtra(EXTRA_THEME)) {
        }
        Theme selectedTheme = (Theme) launchIntent.getSerializableExtra(EXTRA_THEME);

        for (Theme theme : Theme.values()) {
            View view = LayoutInflater.from(this).inflate(R.layout.item_theme, themeContainer, false);

            view.setOnClickListener(new View.OnClickListener() {
                @Override
                public void onClick(View v) {
                    Intent data = new Intent();
                    data.putExtra(EXTRA_THEME, theme);
                    setResult(Activity.RESULT_OK, data);

                    finish();
                }
            });

            TextView themeItemTitle = view.findViewById(R.id.item_theme);
            themeItemTitle.setText(theme.getName());

            ImageView check = view.findViewById(R.id.check);
            if (theme.equals(selectedTheme)) {
                check.setVisibility(View.VISIBLE);
            } else {
                check.setVisibility(View.GONE);
            }

            themeContainer.addView(view);
        }
    }
}