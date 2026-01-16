package com.example.gridcalculator;

import androidx.appcompat.app.AppCompatActivity;

import android.os.Bundle;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;

public class MainActivity extends AppCompatActivity {

    Button b1,buttonEqual,buttonDiv,buttonMul,b5,b6,b7,buttonSub,b9,b10,b11,buttonAdd,b13,b14,b15,b16;
    TextView tv1;
    boolean add,sub,mul,div;
    float n1,n2;
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);
        b1 = (Button) findViewById(R.id.b1);
        buttonEqual = (Button) findViewById(R.id.b2);
        buttonDiv = (Button) findViewById(R.id.b3);
        buttonMul = (Button) findViewById(R.id.b4);
        b5 = (Button) findViewById(R.id.b5);
        b6 = (Button) findViewById(R.id.b6);
        b7 = (Button) findViewById(R.id.b7);
        buttonSub = (Button) findViewById(R.id.b8);
        b9 = (Button) findViewById(R.id.b9);
        b10 = (Button) findViewById(R.id.b10);
        b11 = (Button) findViewById(R.id.b11);
        buttonAdd = (Button) findViewById(R.id.b12);
        b13= (Button) findViewById(R.id.b13);
        b14 = (Button) findViewById(R.id.b14);
        b15 = (Button) findViewById(R.id.b15);
        b16 = (Button) findViewById(R.id.b16);
        tv1 = (TextView) findViewById(R.id.tv1);

        b13.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                tv1.setText(tv1.getText() + "1");
            }
        });
        b14.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                tv1.setText(tv1.getText() + "2");
            }
        });
        b15.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                tv1.setText(tv1.getText() + "3");
            }
        });
        b9.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                tv1.setText(tv1.getText() + "4");
            }
        });
        b10.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                tv1.setText(tv1.getText() + "5");
            }
        });

        b11.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                tv1.setText(tv1.getText() + "6");
            }
        });
        b5.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                tv1.setText(tv1.getText() + "7");
            }
        });
        b6.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                tv1.setText(tv1.getText() + "8");
            }
        });
        b7.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                tv1.setText(tv1.getText() + "9");
            }
        });
        b16.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                tv1.setText(tv1.getText() + "0");
            }
        });
        buttonAdd.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {

                if (tv1 == null) {
                    tv1.setText("");
                } else {
                    n1 = Float.parseFloat(tv1.getText() + "");
                    add = true;
                    tv1.setText(null);
                }
            }
        });

        buttonSub.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                n1 = Float.parseFloat(tv1.getText() + "");
                sub = true;
                tv1.setText(null);
            }
        });
        buttonMul.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                n1 = Float.parseFloat(tv1.getText() + "");
                mul = true;
                tv1.setText(null);
            }
        });
        buttonDiv.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                n1 = Float.parseFloat(tv1.getText() + "");
                div = true;
                tv1.setText(null);
            }
        });
        buttonEqual.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                n2 = Float.parseFloat(tv1.getText() + "");

                if (add == true) {
                    tv1.setText(n1 + n2 + "");
                    add = false;
                }

                if (sub == true) {
                    tv1.setText(n1 - n2 + "");
                    sub = false;
                }

                if (mul == true) {
                    tv1.setText(n1 * n2 + "");
                    mul = false;
                }

                if (div== true) {
                    tv1.setText(n1/ n2 + "");
                    div = false;
                }
            }
        });
        b1.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                tv1.setText("");
            }
        });
    }
}