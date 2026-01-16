package com.sketch.shield.activity;

import android.os.Build;
import android.os.Bundle;
import android.support.v4.content.ContextCompat;
import android.support.v7.app.AppCompatActivity;
import android.view.View;
import android.view.Window;
import android.view.WindowManager;
import android.widget.Button;
import android.widget.EditText;
import android.widget.RelativeLayout;
import android.widget.TextView;

import com.sketch.shield.R;

import de.hdodenhof.circleimageview.CircleImageView;

public class AddCar extends AppCompatActivity
        implements View.OnClickListener {

    RelativeLayout rel_back;
    EditText edt_car_name, edt_car_color, edt_car_no;
    CircleImageView iv_user;
    Button btn_submit;
    TextView tv1;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.add_car);
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.LOLLIPOP) {
            Window window = getWindow();
            window.addFlags(WindowManager.LayoutParams.FLAG_DRAWS_SYSTEM_BAR_BACKGROUNDS);
            window.setStatusBarColor(ContextCompat.getColor(getApplicationContext(),
                    R.color.colorPrimaryDark));
        }

        initViews();


    }

    private void initViews(){

        rel_back = findViewById(R.id.rel_back);
        iv_user = findViewById(R.id.iv_user);

        edt_car_name = findViewById(R.id.edt_car_name);
        edt_car_color = findViewById(R.id.edt_car_color);
        edt_car_no = findViewById(R.id.edt_car_no);



        btn_submit = findViewById(R.id.btn_submit);
        tv1 = findViewById(R.id.tv1);
        //tv1.setText("Add Car");


        rel_back.setOnClickListener(this);
        btn_submit.setOnClickListener(this);


    }

    @Override
    public void onClick(View v) {

        switch (v.getId()){

            case R.id.rel_back:

                finish();

                break;

            case R.id.btn_submit:

                finish();

                break;

        }

    }




}