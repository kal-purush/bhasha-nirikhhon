package com.example.shini_000.musiclibrary;

import android.app.Activity;
import android.content.Intent;
import android.os.Bundle;
import android.util.Log;
import android.widget.ListView;

import Adapter.AlbumAdapter;
import Data.ResultData;
import Objects.Album;
import Objects.Artist;
import Objects.ListALbum;


public class AlbumScreen extends Activity {

    private ListView listView;
    private ListALbum listALbum;
    private AlbumAdapter albumAdapter;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.list_album);

        Intent intent = getIntent();
        Artist artist = intent.getParcelableExtra("artist");

        listView = (ListView) findViewById(R.id.listView);
        listALbum = new ListALbum();
        new ResultData(this, listALbum, listView, artist.getImageArtist()).execute(artist.getLink());
    }
}