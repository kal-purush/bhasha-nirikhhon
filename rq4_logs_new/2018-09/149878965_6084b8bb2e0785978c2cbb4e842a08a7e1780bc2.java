package com.example.computer.event;

import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;

public class ActivityisListener extends AppCompatActivity implements View.OnClickListener {
    private Button btn_tong;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);
        btn_tong = findViewById(R.id.btn_tong);
        btn_tong.setOnClickListener(this);

    }

    @Override
    public void onClick(View v) {
        EditText edta=findViewById(R.id.edt_soa);
        Integer a=Integer.parseInt(edta.getText()+"");
        EditText edtb=findViewById(R.id.edt_sob);
        Integer b=Integer.parseInt(edtb.getText()+"");
        TextView tong=findViewById(R.id.edt_da);
        tong.setText((a+b)+"");
    }
}