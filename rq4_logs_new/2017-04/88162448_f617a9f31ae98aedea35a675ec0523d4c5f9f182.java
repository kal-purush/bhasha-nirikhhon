package br.pro.hashi.ensino.desagil.morse;

import android.content.Intent;
import android.os.Bundle;
import android.view.View;
import android.widget.Button;
import android.widget.TextView;

public class MainActivity extends UtilityActivity {
    private TextView titleText;
    private Button button;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);

        titleText = (TextView) findViewById(R.id.titleText);
        button = (Button) findViewById(R.id.button);

    }

    public void seeTheTruth(View v){
        Utilities.confirm(this,"do you wanna see the truth?");
    }

    public void listenConfirm(boolean answer){

        if(answer){
            startActivity(new Intent(this, SelectorActivity.class));
        }
        else{
            titleText.setText("ok then");
        }

    }
}