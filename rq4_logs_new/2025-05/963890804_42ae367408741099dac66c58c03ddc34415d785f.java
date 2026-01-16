package com.example.musicapp.activity;

import android.annotation.SuppressLint;
import android.os.Bundle;
import android.widget.ImageButton;
import android.widget.TextView;

import androidx.appcompat.app.AppCompatActivity;

import com.example.musicapp.R;
import com.example.musicapp.dto.SongDTO;

import java.text.NumberFormat;
import java.util.Locale;

public class SongDetailActivity extends AppCompatActivity {

    @SuppressLint("SetTextI18n") //bỏ qua cảnh báo của Lint khi bạn thiết lập
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_song_detail);

        // Khởi tạo các View trong layout
        TextView title = findViewById(R.id.songTitle);
        TextView artist = findViewById(R.id.songArtist);
        TextView lyrics = findViewById(R.id.songlyrics);
        TextView description = findViewById(R.id.songDescription);
        TextView license = findViewById(R.id.songLicense);
        TextView viewCount = findViewById(R.id.songViews);
        TextView likeCount = findViewById(R.id.likeCount);
        TextView dislikeCount = findViewById(R.id.dislikeCount);
        ImageButton backButton = findViewById(R.id.backButton);

        // Lấy đối tượng SongDTO từ Intent
        SongDTO song = (SongDTO) getIntent().getSerializableExtra("song");

        // Định dạng số theo chuẩn Việt Nam
        NumberFormat formatter = NumberFormat.getInstance(new Locale("vi", "VN"));

        // Đặt dữ liệu từ đối tượng SongDTO vào các TextView
        if (song != null) {
            title.setText(song.getTitle());
            artist.setText(song.getArtist());
            lyrics.setText(song.getLyrics());
            description.setText("Description: " + song.getDescription());
            license.setText("License: " +song.getLicense());
            viewCount.setText(formatter.format(song.getViews()) + " views");
            likeCount.setText(formatter.format(song.getLikes()));
            dislikeCount.setText(formatter.format(song.getDislikes()));
        }


        backButton.setOnClickListener(v -> finish());
    }
}