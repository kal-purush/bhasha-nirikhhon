package com.cryptojobboard.cryptojobboard;

import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;
import android.widget.Toast;

public class PurchaseEthereumActivity extends AppCompatActivity {

    TextView tvPurchase;
    EditText etAmount;
    Button buttonSubmit;
    String amount;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_purchase_ethereum);

        tvPurchase = findViewById(R.id.tvPurchase);
        etAmount = findViewById(R.id.etAmount);
        buttonSubmit = findViewById(R.id.buttonSubmit);

        buttonSubmit.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                amount = etAmount.getText().toString();
                if(amount.length() > 0) {
                    Toast.makeText(getApplicationContext(), "Thank you for your purchase of " + amount + ".",
                            Toast.LENGTH_SHORT).show();
                } else {
                    Toast.makeText(getApplicationContext(), "Please enter an amount.",
                            Toast.LENGTH_SHORT).show();
                }
            }
        });
    }
}