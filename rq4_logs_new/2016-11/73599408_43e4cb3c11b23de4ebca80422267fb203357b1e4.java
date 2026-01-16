package com.example.jason.linearalgebraapp.Activities.Activities;

import android.app.Activity;
import android.os.Bundle;
import android.view.View;
import android.widget.EditText;
import android.widget.Button;
import com.example.jason.linearalgebraapp.R;

/**
 * Created by jason on 11/13/2016.
 */

public class OrthogonalProjections extends Activity {

    @Override
    protected void onCreate(Bundle savedInstanceState) {
    Button b = (Button) findViewById(R.id.CalcButton);

        b.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                int sum = 0;
                for (int i = 0; i < vector1.getChildCount(); i++) {
                    EditText current = (EditText) vector1.getChildAt(i);
                    EditText current2 = (EditText) vector2.getChildAt(i);

                    sum += Integer.parseInt(current.getText().toString()) * Integer.parseInt(current2.getText().toString());
                }
                dotProduct.setText(String.valueOf(sum));
            }
        });
    }
}