package wav.wav;


import android.Manifest;
import android.content.ContentResolver;
import android.content.Intent;
import android.content.pm.PackageManager;
import android.database.Cursor;
import android.net.Uri;
import android.os.Build;
import android.os.Bundle;
import android.provider.MediaStore;
import android.support.v7.app.AppCompatActivity;
import android.view.View;
import android.widget.ListView;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import android.content.Context;
import android.support.v4.view.ViewPager;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.support.v7.widget.DefaultItemAnimator;
import android.support.v7.widget.LinearLayoutManager;
import android.support.v7.widget.RecyclerView;

import java.util.ArrayList;
import java.util.List;


public class Library extends AppCompatActivity {

   // protected ArrayList<Song> songList;
    protected ListView songView;
    protected MusicService musicSrv;
    protected Intent playIntent;
    protected boolean musicBound = false;
    protected MusicController controller;
    protected boolean paused = false, playbackPaused = false;



    private ArrayList<Song> songList = new ArrayList<>();
   //private List<Movie> movieList = new ArrayList<>();
    private RecyclerView recyclerView;
    private MediaAdapter mAdapter;
    private ViewPager viewPager;
    int images[] = {R.drawable.songs, R.drawable.artists, R.drawable.albums};
    private MyCustomPagerAdapter myCustomPagerAdapter;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.library);
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M) {
            if (checkSelfPermission(Manifest.permission.READ_EXTERNAL_STORAGE)
                    != PackageManager.PERMISSION_GRANTED) {

                requestPermissions(new String[]{Manifest.permission.READ_EXTERNAL_STORAGE}, 1);
                // MY_PERMISSIONS_REQUEST_READ_EXTERNAL_STORAGE is an
                // app-defined int constant
                return;
            }
        }

        viewPager = (ViewPager) findViewById(R.id.viewpager);
        viewPager.setClipToPadding(false);
        int padding = dp2px(this, 32);
        viewPager.setPadding(padding, 0, padding, 0);
        int margin = dp2px(this, 24);
        viewPager.setPageMargin(margin);
        myCustomPagerAdapter = new MyCustomPagerAdapter(Library.this, images);
        viewPager.setAdapter(myCustomPagerAdapter);

        recyclerView = (RecyclerView) findViewById(R.id.recycler_view);
        mAdapter = new MediaAdapter(songList);
        RecyclerView.LayoutManager mLayoutManager = new LinearLayoutManager(getApplicationContext());
        recyclerView.setLayoutManager(mLayoutManager);
        recyclerView.setItemAnimator(new DefaultItemAnimator());
        recyclerView.setAdapter(mAdapter);
        getSongList();
    }


    public void getSongList() {
        //retrieve song info
        ContentResolver musicResolver = getContentResolver();
        Uri musicUri = MediaStore.Audio.Media.EXTERNAL_CONTENT_URI;
        String selection = MediaStore.Audio.Media.IS_MUSIC + "!= 0";
        String sortOrder = MediaStore.Audio.Media.TITLE + " ASC";
        Cursor musicCursor = musicResolver.query(musicUri, null, selection, null, sortOrder);

        if (musicCursor != null && musicCursor.moveToFirst()) {
            //get columns
            int titleColumn = musicCursor.getColumnIndex
                    (MediaStore.Audio.Media.TITLE);
            int idColumn = musicCursor.getColumnIndex
                    (MediaStore.Audio.Media._ID);
            int artistColumn = musicCursor.getColumnIndex
                    (MediaStore.Audio.Media.ARTIST);

            int durationColumn = musicCursor.getColumnIndex(MediaStore.Audio.Media.DURATION);

            int albumColumn = musicCursor.getColumnIndex(MediaStore.Audio.Media.ALBUM);

            int albumArtIDColumn = musicCursor.getColumnIndex(MediaStore.Audio.Media.ALBUM_ID);

            //add songs to list
            do {
                long thisId = musicCursor.getLong(idColumn);
                String thisTitle = musicCursor.getString(titleColumn);
                String thisArtist = musicCursor.getString(artistColumn);
                int thisDuration = musicCursor.getInt(durationColumn);
                String thisAlbum = musicCursor.getString(albumColumn);
                long thisAlbumArtID = musicCursor.getLong(albumArtIDColumn);
                songList.add(new Song(thisId, thisTitle, thisArtist, thisDuration, thisAlbum, getCoverArtPath(musicResolver, thisAlbumArtID)));
            }
            while (musicCursor.moveToNext());
        }
        mAdapter.notifyDataSetChanged();
    }


    private static String getCoverArtPath(ContentResolver musicResolver, long androidAlbumId) {
        String path = null;
        Cursor c = musicResolver.query(
                MediaStore.Audio.Albums.EXTERNAL_CONTENT_URI,
                new String[]{MediaStore.Audio.Albums.ALBUM_ART},
                MediaStore.Audio.Albums._ID + "=?",
                new String[]{Long.toString(androidAlbumId)},
                null);
        if (c != null) {
            if (c.moveToFirst()) {
                path = c.getString(0);
            }
            c.close();
        }
        return path;
    }

    public void songPicked(View view) {
        startActivity(new Intent(Library.this, Playing.class).putExtra("SetSong", Integer.toString(Integer.parseInt(view.getTag().toString()))).putExtra("songList", songList));

    }

    public int dp2px(Context context, int dp ) {
        float m = context.getResources().getDisplayMetrics().density;
        return (int) (dp * m + 0.5f);
    }

    public void toNowPlaying(View view) {
        startActivity(new Intent(Library.this, Playing.class).addFlags(Intent.FLAG_ACTIVITY_REORDER_TO_FRONT));
    }

}