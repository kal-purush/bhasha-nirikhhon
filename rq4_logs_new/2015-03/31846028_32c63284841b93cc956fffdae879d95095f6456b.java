package com.example.tamas.callmanager;

import android.support.v7.app.ActionBarActivity;
import android.os.Bundle;
import android.view.Menu;
import android.view.MenuItem;
import android.widget.Adapter;
import android.widget.ListView;

import java.util.ArrayList;
import java.util.List;


public class MainActivity extends ActionBarActivity {

    private ListView settingsView;
    private SettingsAdapter settingsAdapter;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);

        List<Beallit> settings = new ArrayList<Beallit>();

        settings.add(new Beallit("Profil típus","Éjjel"));
        settings.add(new Beallit("Kezdés időpontja","A profil indulásának időpontja"));
        settings.add(new Beallit("Befejezés időpontja","A profil befejezésének időpontja"));
        settings.add(new Beallit("Hang profil","Csendes"));
        settings.add(new Beallit("Szűrés","Csak a whitelist-esek fogadása"));

        settingsAdapter = new SettingsAdapter();

        settingsView = (ListView) settingsView.findViewById(R.id.settingList);
        settingsView.setAdapter(settingsAdapter);









    }



}