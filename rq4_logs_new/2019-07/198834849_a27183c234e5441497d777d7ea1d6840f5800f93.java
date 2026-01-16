package com.example.customadapterlistviewutimate;

import androidx.appcompat.app.AppCompatActivity;

import android.os.Bundle;
import android.widget.ListView;

import java.util.ArrayList;

public class MainActivity extends AppCompatActivity {

    private ListView lvMusic;
    private ArrayList<Music> musicArrayList;
    private MusicAdapter musicAdapter;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);
        
        
        Radiation();
        CreateAdapter();
    }

    private void CreateAdapter() {
        musicArrayList = new ArrayList<Music>();
        for(int i = 0 ; i< 15; i++){
            musicArrayList.add(new Music("Bài Hát " + i,"10:" + i));
        }

        musicAdapter = new MusicAdapter(MainActivity.this,musicArrayList);
        lvMusic.setAdapter(musicAdapter);

    }

    private void Radiation() {
        lvMusic = (ListView) findViewById(R.id.lvMusic);

    }
}