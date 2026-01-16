package com.example.intenciones;

import androidx.appcompat.app.AppCompatActivity;

import android.content.Intent;
import android.graphics.Bitmap;
import android.graphics.BitmapFactory;
import android.net.Uri;
import android.os.Bundle;
import android.os.Environment;
import android.view.View;
import android.widget.AdapterView;
import android.widget.GridView;
import android.widget.ImageView;

import java.io.File;

public class GalleryPhotosActivity extends AppCompatActivity implements AdapterView.OnItemClickListener {
    private ImageAdapter imageAdapter;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_gallery_photos);
        imageAdapter = new ImageAdapter(this);
        String externalStorage = Environment.getExternalStoragePublicDirectory(
                Environment.DIRECTORY_DCIM
        ).getAbsolutePath();
        externalStorage += File.separator + "Camera" + File.separator;

        File targetDir = new File(externalStorage);
        File[] files = targetDir.listFiles();
        if (files != null) {
            for (File file: files) {
                imageAdapter.Add(file.getAbsolutePath());
            }
        }

        GridView gridView = findViewById(R.id.gridView);
        gridView.setAdapter(imageAdapter);
        gridView.setOnItemClickListener(this);
    }

    @Override
    public void onItemClick(AdapterView<?> adapterView, View view, int i, long l) {
        String path = imageAdapter.getItem(i).toString();
        BitmapFactory.Options options = new BitmapFactory.Options();
        options.inSampleSize = 4;

        Bitmap bitmap = BitmapFactory.decodeFile(path, options);
        Intent intent = new Intent(this, ShowImage.class);
        intent.putExtra("data", bitmap);
        startActivity(intent);
    }
}