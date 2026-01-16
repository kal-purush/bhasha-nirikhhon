package es.gameapp.multigameapp;

import androidx.appcompat.app.AppCompatActivity;

import android.content.Intent;
import android.os.Bundle;
import android.view.View;
import android.widget.Button;

import javax.security.auth.SubjectDomainCombiner;

import es.gameapp.multigameapp.blackjack.BlackActivity;
import es.gameapp.multigameapp.hanoi.HanoyActivity;
import es.gameapp.multigameapp.simon.SimonActivity;
import es.gameapp.multigameapp.sopa.SopaActivity;
import es.gameapp.multigameapp.sudoku.SudokuActivity;
import es.gameapp.multigameapp.trivia.TriviaActivity;

public class MenuActivity extends AppCompatActivity {
    //this activity let you choose between several games
    private Button btnBlack;
    private Button btnHanoi;
    private Button btnSimon;
    private Button btnSopa;
    private Button btnSudoku;
    private Button btnTrivia;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_menu);
        btnBlack = findViewById(R.id.btn_menu_black);
        btnHanoi = findViewById(R.id.btn_menu_hanoi);
        btnSimon = findViewById(R.id.btn_menu_simon);
        btnSopa = findViewById(R.id.btn_menu_sopa);
        btnSudoku = findViewById(R.id.btn_menu_sudoku);
        btnTrivia = findViewById(R.id.btn_menu_trivia);

        //Intents
        btnBlack.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                Intent intento = new Intent(MenuActivity.this, BlackActivity.class);
                startActivity(intento);
            }
        });
        btnHanoi.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                Intent intento = new Intent(MenuActivity.this, HanoyActivity.class);
                startActivity(intento);
            }
        });
        btnSimon.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                Intent intento = new Intent(MenuActivity.this, SimonActivity.class);
                startActivity(intento);
            }
        });
        btnSopa.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                Intent intento = new Intent(MenuActivity.this, SopaActivity.class);
                startActivity(intento);
            }
        });
        btnSudoku.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                Intent intento = new Intent(MenuActivity.this, SudokuActivity.class);
                startActivity(intento);
            }
        });
        btnTrivia.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                Intent intento = new Intent(MenuActivity.this, TriviaActivity.class);
                startActivity(intento);
            }
        });
        //End Intents
    }


}