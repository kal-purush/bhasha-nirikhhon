package com.example.arondaniel.lolpickshelp;

import android.media.Image;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.util.Log;
import android.widget.ImageView;

public class ChampionScreenActivity extends AppCompatActivity {

    private static String TAG = "ChampionScreen";

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_champion_screen);

        String championName = getIntent().getStringExtra(HomeScreenActivity.CHAMPION_NAME);
        Log.d(TAG, championName);

        ImageView championPortrait = (ImageView) findViewById(R.id.championPortrait);



        // O trecho abaixo pega o identificador  do icone do campeão selecionado
        int idChampionIcon = this.getResources().getIdentifier(championName, "drawable", this.getPackageName());

       //Setando o ícone do campeão
        championPortrait.setImageResource(idChampionIcon);

    }
}