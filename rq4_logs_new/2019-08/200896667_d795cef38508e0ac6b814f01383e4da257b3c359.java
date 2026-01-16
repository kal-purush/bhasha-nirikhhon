package com.example.rockpaperscissors;

import java.util.Random;

import android.os.Bundle;
import android.app.Activity;
import android.app.AlertDialog;
import android.content.DialogInterface;
import android.content.Intent;
import android.view.Menu;
import android.view.View;
import android.view.View.OnClickListener;
import android.widget.Button;
import android.widget.ImageButton;
import android.widget.ImageView;
import android.widget.TextView;

public class AdvancedGameActivity extends Activity implements OnClickListener {

    public enum Option {
        ROCK,
        PAPER,
        SCISSORS,
        LIZARD,
        SPOCK
    }

    public enum Result {
        WIN,
        LOSS,
        DRAW
    }
    private Option userSelection, androidSelection;
    int uc = 0;
    int ac = 0;
    Button bt1, bt2, bt3, bt4, bt5;
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_advanced_game);
        bt1 = (Button) findViewById(R.id.advanced_rock);
        bt2 = (Button) findViewById(R.id.advanced_paper);
        bt3 = (Button) findViewById(R.id.advanced_scissors);
        bt4 = (Button) findViewById(R.id.advanced_lizard);
        bt5 = (Button) findViewById(R.id.advanced_spock);
        bt1.setOnClickListener(this);
        bt2.setOnClickListener(this);
        bt3.setOnClickListener(this);
        bt4.setOnClickListener(this);
        bt5.setOnClickListener(this);
    }

    @Override
    public void onClick(View v) {
        // TODO Auto-generated method stub


    }

    

   

}