package com.example.mi_b_wizard;

import android.content.Intent;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.view.View;
import android.widget.RadioButton;
import android.widget.RadioGroup;
import android.widget.Toast;


public class NewGameActivity extends AppCompatActivity {

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_new_game);

        RadioGroup radioGroup = (RadioGroup) findViewById(R.id.rdbGp1);
        radioGroup.setOnCheckedChangeListener(new RadioGroup.OnCheckedChangeListener() {
            public void onCheckedChanged(RadioGroup group, int checkedId) {
                RadioButton zweisp = (RadioButton) findViewById(R.id.zweispieler);
                RadioButton dreisp = (RadioButton) findViewById(R.id.dreispieler);
                RadioButton viersp = (RadioButton) findViewById(R.id.vierspieler);
                if (zweisp.isChecked()){
                    DisplayToast("2 Spieler ausgewählt.");
                }
                else if (dreisp.isChecked()){
                    DisplayToast("3 Spieler ausgewählt.");
                }
                else {
                    DisplayToast("4 Spieler ausgewählt.");
                }
            }
        });

/*
        //Wechsel zu Activity, wo weitere Spieler dazukommen

        ButtonStart = findViewById(R.id.start);//Wechsel zu ...
        ButtonStart.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                Intent i = new Intent(NewGameActivity.this, MUSTER.class);
                startActivity(i);
            }
        });*/
    }


    private void DisplayToast (String msg){
        Toast.makeText(getBaseContext(), msg, Toast.LENGTH_SHORT).show();
    }
}

