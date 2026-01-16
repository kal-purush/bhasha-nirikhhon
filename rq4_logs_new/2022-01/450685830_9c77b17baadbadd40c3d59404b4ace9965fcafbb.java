package com.example.calculatorgb;

import android.annotation.SuppressLint;
import android.os.Bundle;
import android.view.View;
import android.widget.TextView;

import androidx.annotation.Nullable;
import androidx.appcompat.app.AppCompatActivity;

import java.util.Locale;

public class CalcActivity extends AppCompatActivity implements View.OnClickListener {

    private TextView resultString;
    private TextView operationString;
    private Float result = 0f;
    private Float number = 0f;
    private boolean dotFlag = false;
    private String operation = null;

    @Override
    protected void onCreate(@Nullable Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.calc_layout);

        resultString = findViewById(R.id.result_string);
        operationString = findViewById(R.id.operation_string);

        findViewById(R.id.key_one).setOnClickListener(this);
        findViewById(R.id.key_dot).setOnClickListener(this);
        findViewById(R.id.key_eight).setOnClickListener(this);
        findViewById(R.id.key_five).setOnClickListener(this);
        findViewById(R.id.key_four).setOnClickListener(this);
        findViewById(R.id.key_minus).setOnClickListener(this);
        findViewById(R.id.key_multiply).setOnClickListener(this);
        findViewById(R.id.key_nine).setOnClickListener(this);
        findViewById(R.id.key_plus).setOnClickListener(this);
        findViewById(R.id.key_reset).setOnClickListener(this);
        findViewById(R.id.key_result).setOnClickListener(this);
        findViewById(R.id.key_seven).setOnClickListener(this);
        findViewById(R.id.key_six).setOnClickListener(this);
        findViewById(R.id.key_split).setOnClickListener(this);
        findViewById(R.id.key_three).setOnClickListener(this);
        findViewById(R.id.key_two).setOnClickListener(this);
        findViewById(R.id.key_zero).setOnClickListener(this);

    }


    @SuppressLint("NonConstantResourceId")
    @Override
    public void onClick(View view) {
        switch (view.getId()) {
            case R.id.key_one:
                if (resultString.getText().toString().equals("0")) {
                    resultString.setText(R.string.key_one);
                    result = Float.parseFloat(resultString.getText().toString());
                } else {
                    resultString.setText(String.format(Locale.getDefault(), "%s", resultString.getText().toString() + "1"));
                    result = Float.parseFloat(resultString.getText().toString());
                }
                break;

            case R.id.key_two:
                if (resultString.getText().toString().equals("0")) {
                    resultString.setText(R.string.key_two);
                    result = Float.parseFloat(resultString.getText().toString());
                } else {
                    resultString.setText(String.format(Locale.getDefault(), "%s", resultString.getText().toString() + "2"));
                    result = Float.parseFloat(resultString.getText().toString());
                }
                break;

            case R.id.key_three:
                if (resultString.getText().toString().equals("0")) {
                    resultString.setText(R.string.key_three);
                    result = Float.parseFloat(resultString.getText().toString());
                } else {
                    resultString.setText(String.format(Locale.getDefault(), "%s", resultString.getText().toString() + "3"));
                    result = Float.parseFloat(resultString.getText().toString());
                }
                break;

            case R.id.key_four:
                if (resultString.getText().toString().equals("0")) {
                    resultString.setText(R.string.key_four);
                    result = Float.parseFloat(resultString.getText().toString());
                } else {
                    resultString.setText(String.format(Locale.getDefault(), "%s", resultString.getText().toString() + "4"));
                    result = Float.parseFloat(resultString.getText().toString());
                }
                break;

            case R.id.key_five:
                if (resultString.getText().toString().equals("0")) {
                    resultString.setText(R.string.key_five);
                    result = Float.parseFloat(resultString.getText().toString());
                } else {
                    resultString.setText(String.format(Locale.getDefault(), "%s", resultString.getText().toString() + "5"));
                    result = Float.parseFloat(resultString.getText().toString());
                }
                break;

            case R.id.key_six:
                if (resultString.getText().toString().equals("0")) {
                    resultString.setText(R.string.key_six);
                    result = Float.parseFloat(resultString.getText().toString());
                } else {
                    resultString.setText(String.format(Locale.getDefault(), "%s", resultString.getText().toString() + "6"));
                    result = Float.parseFloat(resultString.getText().toString());
                }
                break;

            case R.id.key_seven:
                if (resultString.getText().toString().equals("0")) {
                    resultString.setText(R.string.key_seven);
                    result = Float.parseFloat(resultString.getText().toString());
                } else {
                    resultString.setText(String.format(Locale.getDefault(), "%s", resultString.getText().toString() + "7"));
                    result = Float.parseFloat(resultString.getText().toString());
                }
                break;

            case R.id.key_eight:
                if (resultString.getText().toString().equals("0")) {
                    resultString.setText(R.string.key_eight);
                    result = Float.parseFloat(resultString.getText().toString());
                } else {
                    resultString.setText(String.format(Locale.getDefault(), "%s", resultString.getText().toString() + "8"));
                    result = Float.parseFloat(resultString.getText().toString());
                }
                break;

            case R.id.key_nine:
                if (resultString.getText().toString().equals("0")) {
                    resultString.setText(R.string.key_nine);
                    result = Float.parseFloat(resultString.getText().toString());
                } else {
                    resultString.setText(String.format(Locale.getDefault(), "%s", resultString.getText().toString() + "9"));
                    result = Float.parseFloat(resultString.getText().toString());
                }
                break;

            case R.id.key_zero:
                if (resultString.getText().toString().equals("0")) {
                    result = Float.parseFloat(resultString.getText().toString());
                } else {
                    resultString.setText(String.format(Locale.getDefault(), "%s", resultString.getText().toString() + "0"));
                    result = Float.parseFloat(resultString.getText().toString());
                }
                break;

            case R.id.key_dot:
                if (dotFlag) {
                    break;
                } else {
                    resultString.setText(String.format(Locale.getDefault(), "%s", resultString.getText().toString() + "."));
                    result = Float.parseFloat(resultString.getText().toString() + "0");
                    dotFlag = true;
                }
                break;

            case R.id.key_plus:
                operation = "+";
                number = Float.parseFloat(resultString.getText().toString());
                operationString.setText(String.format(Locale.getDefault(), "%s", resultString.getText().toString() + " + "));
                resultString.setText("0");
                dotFlag = false;
                break;

            case R.id.key_minus:
                operation = "-";
                number = Float.parseFloat(resultString.getText().toString());
                operationString.setText(String.format(Locale.getDefault(), "%s", resultString.getText().toString() + " - "));
                resultString.setText("0");
                dotFlag = false;
                break;

            case R.id.key_multiply:
                operation = "x";
                number = Float.parseFloat(resultString.getText().toString());
                operationString.setText(String.format(Locale.getDefault(), "%s", resultString.getText().toString() + " x "));
                resultString.setText("0");
                dotFlag = false;
                break;

            case R.id.key_split:
                operation = "/";
                number = Float.parseFloat(resultString.getText().toString());
                operationString.setText(String.format(Locale.getDefault(), "%s", resultString.getText().toString() + " / "));
                resultString.setText("0");
                dotFlag = false;
                break;

            case R.id.key_result:
                switch (operation) {

                    case "+":
                        number = number + Float.parseFloat(resultString.getText().toString());
                        break;

                    case "-":
                        number = number - Float.parseFloat(resultString.getText().toString());
                        break;

                    case "x":
                        number = number * Float.parseFloat(resultString.getText().toString());
                        break;

                    case "/":
                        number = number / Float.parseFloat(resultString.getText().toString());
                        break;

                }
                operationString.setText(null);
                resultString.setText(String.valueOf(number));
                result = 0f;
                number = 0f;
                dotFlag = false;
                operation = null;
                break;

            case R.id.key_reset:
                operationString.setText(null);
                resultString.setText("0");
                result = 0f;
                number = 0f;
                dotFlag = false;
                operation = null;
                break;


        }
    }
}