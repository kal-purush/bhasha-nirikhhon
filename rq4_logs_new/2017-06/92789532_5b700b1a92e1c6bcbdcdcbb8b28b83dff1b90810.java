package com.dell.juliana.filme;

import android.content.Intent;
import android.os.Bundle;
import android.support.v7.app.AppCompatActivity;
import android.support.v7.widget.Toolbar;
import android.view.Menu;
import android.view.MenuInflater;
import android.view.MenuItem;

import com.dell.juliana.filme.fragments.FilmeFormFragments;
import com.dell.juliana.filme.fragments.ListFilmesFragments;

/**
 * Created by Juliana on 18/06/2017.
 */

public class ListFilmesActivity extends AppCompatActivity  implements FilmeFormFragments.OnRefreshFormOK {

    ListFilmesFragments fragmentList;
    private Toolbar toolbarLayout;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_list_filme);
        fragmentList = (ListFilmesFragments) getSupportFragmentManager().findFragmentById(R.id.fragmentList);
        toolbarLayout = (Toolbar)findViewById(R.id.toolbar);
        setSupportActionBar(toolbarLayout); //Setanto a toolbar
    }
    @Override
    public void refresh(){
        fragmentList.loadFilmes();
    }

    //Para Fazer algo na Toolbar usamos onCreateOptionsMenu
    @Override
    public boolean onCreateOptionsMenu(Menu menu){
        MenuInflater inflater = getMenuInflater();//Criamos Variável que pega um mu inflateren
        inflater.inflate(R.menu.menu_toolbar, menu);//referenciando enu que Criamoso
        return true;
    }

    @Override
    public boolean onOptionsItemSelected(MenuItem item){//Aqui vai Detectar o Menu
        Intent it;
        switch (item.getItemId()){
            case R.id.botaoMap:
                it = new Intent(this, MapsActivity.class);
                startActivity(it);
                break;
            case R.id.btnPager:
                it = new Intent(this, ViewPagerFilmeActivity.class);
                startActivity(it);
        }
        return super.onOptionsItemSelected(item);
    }
}