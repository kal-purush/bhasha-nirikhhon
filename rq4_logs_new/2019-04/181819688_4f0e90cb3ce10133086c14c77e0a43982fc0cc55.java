package com.ab.calculator;

import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;
import android.widget.Toast;

public class calc extends AppCompatActivity {
    EditText num_1, num_2;
    Button sum, sub, mul, div, rem;
    TextView result;
    private static final int TIME_DELAY = 2000;
    private static long back_pressed;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.calc);
        num_1 = (EditText) findViewById(R.id.num1);
        num_2 = (EditText) findViewById(R.id.num2);
        sum = (Button) findViewById(R.id.sum);
        sub = (Button) findViewById(R.id.sub);
        mul = (Button) findViewById(R.id.mul);
        div = (Button) findViewById(R.id.div);
        rem = (Button) findViewById(R.id.rem);
        result = (TextView) findViewById(R.id.result);
    }

    public void Summation(View view) {
        if(num_1.getText().toString().equals("")){
            float val_2=Float.parseFloat(num_2.getText().toString());
            result.setText(""+val_2);
        }
        else if(num_2.getText().toString().equals("")){
            float val_1=Float.parseFloat(num_1.getText().toString());
            result.setText(""+val_1);
        }
        else{
            float val_1=Float.parseFloat(num_1.getText().toString());
            float val_2=Float.parseFloat(num_2.getText().toString());
            float res=val_1+val_2;
            result.setText(""+res);
        }
    }
    public void Subtraction(View view) {
        if(num_1.getText().toString().equals("")){
            float val_2=Float.parseFloat(num_2.getText().toString());
            result.setText("-"+val_2);
        }
        else if(num_2.getText().toString().equals("")){
            float val_1=Float.parseFloat(num_1.getText().toString());
            result.setText(""+val_1);
        }
        else{
            float val_1=Float.parseFloat(num_1.getText().toString());
            float val_2=Float.parseFloat(num_2.getText().toString());
            float res=val_1-val_2;
            result.setText(""+res);
        }
    }
    public void Multiplication(View view) {
        if(num_1.getText().toString().equals("")){
            float val_2=0;
            result.setText(""+val_2);
        }
        else if(num_2.getText().toString().equals("")){
            float val_1=0;
            result.setText(""+val_1);
        }
        else{
            float val_1=Float.parseFloat(num_1.getText().toString());
            float val_2=Float.parseFloat(num_2.getText().toString());
            float res=val_1*val_2;
            result.setText(""+res);
        }
    }
    public void Division(View view) {
        if(num_1.getText().toString().equals("")){
            float val_2=0;
            result.setText(""+val_2);
        }
        else if(num_2.getText().toString().equals("")){
            String val_1="Infinite";
            result.setText(val_1);
        }
        else{
            float val_1=Float.parseFloat(num_1.getText().toString());
            float val_2=Float.parseFloat(num_2.getText().toString());
            float res=val_1/val_2;
            result.setText(""+res);
        }
    }
    public void Remainder(View view) {
        if(num_1.getText().toString().equals("")||num_2.getText().toString().equals("")){
            String val_2="Not defind";
            result.setText(val_2);
        }
        else{
            float val_1=Float.parseFloat(num_1.getText().toString());
            float val_2=Float.parseFloat(num_2.getText().toString());
            float res=val_1%val_2;
            result.setText(""+res);
        }
    }
    @Override
    public void onBackPressed() {

        doExit();
    }

    private void doExit() {
        if (back_pressed + TIME_DELAY > System.currentTimeMillis()) {
            super.onBackPressed();
        } else {
            Toast.makeText(getBaseContext(), "Press once again to exit!",
                    Toast.LENGTH_SHORT).show();
        }
        back_pressed = System.currentTimeMillis();
    }
}