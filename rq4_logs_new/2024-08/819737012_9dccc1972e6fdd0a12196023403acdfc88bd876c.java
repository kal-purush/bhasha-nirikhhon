package com.example.duan1_catmusic.Activity;

import android.content.Intent;
import android.os.Bundle;
import android.view.View;
import android.view.WindowManager;
import android.widget.ImageView;

import androidx.activity.EdgeToEdge;
import androidx.appcompat.app.AppCompatActivity;
import androidx.core.graphics.Insets;
import androidx.core.view.ViewCompat;
import androidx.core.view.WindowInsetsCompat;
import androidx.recyclerview.widget.LinearLayoutManager;
import androidx.recyclerview.widget.RecyclerView;

import com.example.duan1_catmusic.Adapter.MusicAdapter;
import com.example.duan1_catmusic.DAO.nhacDAO;
import com.example.duan1_catmusic.R;
import com.example.duan1_catmusic.model.Nhac;

import java.util.ArrayList;
import java.util.List;

public class Screen_ListSong_YeuThich extends AppCompatActivity {

    private List<Nhac> list;
    private nhacDAO nhac_DAO;
    private ImageView back;
    private RecyclerView recyclerView;
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_screen_list_song_yeu_thich);

        back = findViewById(R.id.back);
        recyclerView = findViewById(R.id.rvList_yeuthich);

        back.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                finish();
            }
        });

        getWindow().setFlags(WindowManager.LayoutParams.FLAG_FULLSCREEN, WindowManager.LayoutParams.FLAG_FULLSCREEN);
        Intent intent = getIntent();
        boolean showAllSongs = intent.getBooleanExtra("all_songs", false);
        list = new ArrayList<>();
        nhac_DAO = new nhacDAO(this);
//        if (showAllSongs) {
//            loadAllData();
//        } else {
//            String artistName = intent.getStringExtra("ten_nghe_si");
//            loaddata(artistName);
//        }
        loadAllData();


    }
    private void loaddata(String artistName) {
        // Lọc danh sách bài hát theo tên nghệ sĩ
        list = nhac_DAO.getSongsByArtist(artistName);

        LinearLayoutManager manager = new LinearLayoutManager(this);
        manager.setOrientation(RecyclerView.VERTICAL);
        recyclerView.setLayoutManager(manager);

        MusicAdapter adapter = new MusicAdapter(list, this);
        recyclerView.setAdapter(adapter);

        // Set item click listener to open Screen_listening_music
        adapter.setOnItemClickListener(new MusicAdapter.OnItemClickListener() {
            @Override
            public void onItemClick(int position) {
                Intent intent = new Intent(Screen_ListSong_YeuThich.this, Screen_listening_music.class);
                intent.putExtra("playlist", (ArrayList<Nhac>) list); // Truyền danh sách phát
                intent.putExtra("currentTrackIndex", position); // Truyền vị trí bài hát hiện tại
                startActivity(intent);
            }
        });
    }
    private void loadAllData() {
        // Load all songs
        list = nhac_DAO.getSongArtistList();

        LinearLayoutManager manager = new LinearLayoutManager(this);
        manager.setOrientation(RecyclerView.VERTICAL);
        recyclerView.setLayoutManager(manager);

        MusicAdapter adapter = new MusicAdapter(list, this);
        recyclerView.setAdapter(adapter);

        // Set item click listener to open Screen_listening_music
        adapter.setOnItemClickListener(new MusicAdapter.OnItemClickListener() {
            @Override
            public void onItemClick(int position) {
                Intent intent = new Intent(Screen_ListSong_YeuThich.this, Screen_listening_music.class);
                intent.putExtra("playlist", (ArrayList<Nhac>) list); // Truyền danh sách phát
                intent.putExtra("currentTrackIndex", position); // Truyền vị trí bài hát hiện tại
                startActivity(intent);
            }
        });
    }
}