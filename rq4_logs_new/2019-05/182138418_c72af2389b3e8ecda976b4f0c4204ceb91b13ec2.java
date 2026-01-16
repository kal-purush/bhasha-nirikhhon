package ru.nbdev.androidapp3;

import androidx.appcompat.app.AppCompatActivity;
import android.os.Bundle;
import android.widget.TextView;
import com.google.android.material.bottomnavigation.BottomNavigationView;

public class BottomActivity extends AppCompatActivity {

    private TextView textView;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_bottom);

        textView = findViewById(R.id.bottom_text_view);
        bottomAction1();

        BottomNavigationView bottomNavigationView = findViewById(R.id.bottom_navigation_view);
        bottomNavigationView.setOnNavigationItemSelectedListener(item -> {
            switch (item.getItemId()) {
                case R.id.bottom_action_1:
                    bottomAction1();
                    break;

                case R.id.bottom_action_2:
                    bottomAction2();
                    break;

                case R.id.bottom_action_3:
                    bottomAction3();
                    break;
            }
            return true;
        });
    }

    private void bottomAction1() {
        textView.setText(R.string.bottom_action_1);
    }

    private void bottomAction2() {
        textView.setText(R.string.bottom_action_2);
    }

    private void bottomAction3() {
        textView.setText(R.string.bottom_action_3);
    }
}