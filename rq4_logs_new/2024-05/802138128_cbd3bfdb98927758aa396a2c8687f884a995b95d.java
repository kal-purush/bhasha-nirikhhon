package com.example.study;

import androidx.appcompat.app.AppCompatActivity;

import android.os.Bundle;
import android.view.View;
import android.widget.Adapter;
import android.widget.Button;
import android.widget.Toast;

public class PhysicsActivity extends AppCompatActivity {
    Button btn1,btn2,btn3,btn4 ,btnSb,
    btn5,btn6,btn7,btn8,
    btn9,btn10,btn11,btn12,
    btn13,btn14,btn15,btn16,
    btn17,btn18,btn19,btn20,
    btn21,btn22,btn23,btn24,
    btn25,btn26,btn27,btn28,
    btn29,btn30,btn31,btn32,
    btn33,btn34,btn35,btn36,
   btn37, btn38,btn39,btn40;
    Integer flag;


    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_physics);
//question 1
        btn1 = findViewById(R.id.q1ap);
        btn2 = findViewById(R.id.q1bp);
        btn3 = findViewById(R.id.q1cp);
        btn4 = findViewById(R.id.q1dp);
        btnSb = findViewById(R.id.psb);

        BtnClick CBl = new CorrBtnClick();
        BtnClick Ibl = new incrBtnClick();


        btn1.setOnClickListener(new AdapterClickListener(Ibl));
        btn2.setOnClickListener(new AdapterClickListener(Ibl));
        btn3.setOnClickListener(new AdapterClickListener(CBl));
        btn4.setOnClickListener(new AdapterClickListener(Ibl));
//question 2
        btn5 = findViewById(R.id.q2ap);
        btn6 = findViewById(R.id.q2bp);
        btn7= findViewById(R.id.q2cp);
        btn8 = findViewById(R.id.q2dp);

        btn5.setOnClickListener(new AdapterClickListener(CBl));
        btn6.setOnClickListener(new AdapterClickListener(Ibl));
        btn7.setOnClickListener(new AdapterClickListener(Ibl));
        btn8.setOnClickListener(new AdapterClickListener(Ibl));

        // question 3
        btn9 = findViewById(R.id.q3ap);
        btn10 = findViewById(R.id.q3bp);
        btn11= findViewById(R.id.q3cp);
        btn12= findViewById(R.id.q3dp);

        btn9.setOnClickListener(new AdapterClickListener(Ibl));
        btn10.setOnClickListener(new AdapterClickListener(Ibl));
        btn11.setOnClickListener(new AdapterClickListener(Ibl));
        btn12.setOnClickListener(new AdapterClickListener(CBl));
//question 4
        btn13 = findViewById(R.id.q4pa);
        btn14 = findViewById(R.id.q4pb);
        btn15= findViewById(R.id.q4pc);
        btn16= findViewById(R.id.q4pd);

        btn13.setOnClickListener(new AdapterClickListener(Ibl));
        btn14.setOnClickListener(new AdapterClickListener(CBl));
        btn15.setOnClickListener(new AdapterClickListener(Ibl));
        btn16.setOnClickListener(new AdapterClickListener(Ibl));
//question 5
        btn17 = findViewById(R.id.q5ap);
        btn18 = findViewById(R.id.q5bp);
        btn19= findViewById(R.id.q5cp);
        btn20= findViewById(R.id.q5dp);

        btn17.setOnClickListener(new AdapterClickListener(Ibl));
        btn18.setOnClickListener(new AdapterClickListener(CBl));
        btn19.setOnClickListener(new AdapterClickListener(Ibl));
        btn20.setOnClickListener(new AdapterClickListener(Ibl));
//question 6

        btn21 = findViewById(R.id.q6ap);
        btn22 = findViewById(R.id.q6bp);
        btn23= findViewById(R.id.q6cp);
        btn24= findViewById(R.id.q6dp);

        btn21.setOnClickListener(new AdapterClickListener(Ibl));
        btn22.setOnClickListener(new AdapterClickListener(CBl));
        btn23.setOnClickListener(new AdapterClickListener(Ibl));
        btn24.setOnClickListener(new AdapterClickListener(Ibl));

//question 7

        btn25 = findViewById(R.id.q7ap);
        btn26 = findViewById(R.id.q7bp);
        btn27= findViewById(R.id.q7cp);
        btn28= findViewById(R.id.q7dp);

        btn25.setOnClickListener(new AdapterClickListener(Ibl));
        btn26.setOnClickListener(new AdapterClickListener(Ibl));
        btn27.setOnClickListener(new AdapterClickListener(Ibl));
        btn28.setOnClickListener(new AdapterClickListener(CBl));

 //question 8

        btn29 = findViewById(R.id.q8ap);
        btn30 = findViewById(R.id.q8bp);
        btn31= findViewById(R.id.q8cp);
        btn32= findViewById(R.id.q8dp);

        btn29.setOnClickListener(new AdapterClickListener(Ibl));
        btn30.setOnClickListener(new AdapterClickListener(Ibl));
        btn31.setOnClickListener(new AdapterClickListener(CBl));
        btn32.setOnClickListener(new AdapterClickListener(Ibl));

 //question 9
        btn33= findViewById(R.id.q9ap);
        btn34 = findViewById(R.id.q9bp);
        btn35= findViewById(R.id.q9cp);
        btn36= findViewById(R.id.q9dp);

        btn33.setOnClickListener(new AdapterClickListener(CBl));
        btn34.setOnClickListener(new AdapterClickListener(Ibl));
        btn35.setOnClickListener(new AdapterClickListener(Ibl));
        btn36.setOnClickListener(new AdapterClickListener(Ibl));

 //question 10

        btn37= findViewById(R.id.q10ap);
        btn38 = findViewById(R.id.q10bp);
        btn39= findViewById(R.id.q10cp);
        btn40= findViewById(R.id.q10dp);

        btn33.setOnClickListener(new AdapterClickListener(CBl));
        btn34.setOnClickListener(new AdapterClickListener(Ibl));
        btn35.setOnClickListener(new AdapterClickListener(Ibl));
        btn36.setOnClickListener(new AdapterClickListener(Ibl));


        btnSb.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                Toast.makeText(PhysicsActivity.this, "Your Score:"+ ScoreManager.getInstance().getScore(), Toast.LENGTH_SHORT).show();
            }
        });
    }




    private class AdapterClickListener implements View.OnClickListener {

        private BtnClick btnClick;

        public AdapterClickListener(BtnClick btnClick) {

            this.btnClick = btnClick;
        }

        @Override
        public void onClick(View view) {
            btnClick.onClick();

        }
    }



    }