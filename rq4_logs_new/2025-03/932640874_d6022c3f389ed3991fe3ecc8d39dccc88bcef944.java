package dev.frilly.locket.activities;

import android.content.Intent;
import android.media.MediaPlayer;
import android.net.Uri;
import android.os.Bundle;
import android.view.SurfaceHolder;
import android.view.SurfaceView;
import android.view.View;
import android.widget.ImageView;

import androidx.appcompat.app.AppCompatActivity;

import java.io.File;
import java.io.IOException;

import dev.frilly.locket.R;

public class ConfirmPostActivity extends AppCompatActivity {
    private ImageView imageView;
    private SurfaceView surfaceView;
    private MediaPlayer mediaPlayer;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.confirm_post_screen);

        imageView = findViewById(R.id.imageView);
        surfaceView = findViewById(R.id.surfaceView);

        Intent intent = getIntent();
        if (intent != null) {
            String filePath = intent.getStringExtra("file_path");
            String fileType = intent.getStringExtra("file_type");

            if (filePath != null) {
                if ("image".equals(fileType)) {
                    showImage(filePath);
                } else if ("video".equals(fileType)) {
                    showVideo(filePath);
                }
            }
        }
    }

    private void showImage(String path) {
        imageView.setVisibility(View.VISIBLE);
        surfaceView.setVisibility(View.GONE);
        imageView.setImageURI(Uri.fromFile(new File(path)));
    }

    private void showVideo(String path) {
        imageView.setVisibility(View.GONE);
        surfaceView.setVisibility(View.VISIBLE);

        mediaPlayer = new MediaPlayer();
        SurfaceHolder holder = surfaceView.getHolder();

        holder.addCallback(new SurfaceHolder.Callback() {
            @Override
            public void surfaceCreated(SurfaceHolder holder) {
                try {
                    mediaPlayer.setDisplay(holder);
                    mediaPlayer.setDataSource(path);
                    mediaPlayer.setOnPreparedListener(mp -> {
                        mp.start();
                    });
                    mediaPlayer.prepareAsync();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            @Override
            public void surfaceChanged(SurfaceHolder holder, int format, int width, int height) {}

            @Override
            public void surfaceDestroyed(SurfaceHolder holder) {
                if (mediaPlayer != null) {
                    mediaPlayer.release();
                    mediaPlayer = null;
                }
            }
        });
    }
}