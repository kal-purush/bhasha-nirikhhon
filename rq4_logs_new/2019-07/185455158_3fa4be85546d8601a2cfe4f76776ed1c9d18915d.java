package com.hotsauce.meem;

import android.Manifest;
import android.content.Intent;
import android.content.pm.PackageManager;
import android.os.Bundle;
import android.view.MenuItem;
import android.widget.TextView;

import androidx.annotation.NonNull;
import androidx.annotation.Nullable;
import androidx.appcompat.app.AppCompatActivity;
import androidx.core.app.ActivityCompat;
import androidx.core.content.ContextCompat;
import androidx.lifecycle.Observer;
import androidx.lifecycle.ViewModelProviders;
import androidx.recyclerview.widget.GridLayoutManager;
import androidx.recyclerview.widget.RecyclerView;

import com.google.android.material.bottomnavigation.BottomNavigationView;
import com.hotsauce.meem.db.Meme;
import com.hotsauce.meem.db.MemeTemplate;
import com.hotsauce.meem.state.GreetingContext;

import java.io.File;
import java.util.List;

import static com.hotsauce.meem.PhotoEditor.BaseActivity.READ_WRITE_STORAGE;


public class TemplateGalleryActivity extends AppCompatActivity {

    private TextView mTextMessage;

    private MemeViewModel memeViewModel;

    private BottomNavigationView.OnNavigationItemSelectedListener mOnNavigationItemSelectedListener
            = new BottomNavigationView.OnNavigationItemSelectedListener() {

        @Override
        public boolean onNavigationItemSelected(@NonNull MenuItem item) {
            Intent intent;

            switch (item.getItemId()) {
                case R.id.navigation_home:
                    mTextMessage.setText(R.string.title_home);
                    finish();
                    return true;
                case R.id.navigation_dashboard:
                    intent = new Intent(getApplicationContext(), CreateTemplateActivity.class);
                    startActivity(intent);
                    mTextMessage.setText(R.string.title_dashboard);
                    return true;
                case R.id.navigation_notifications:
                    mTextMessage.setText(R.string.title_notifications);
                    return true;
            }
            return false;
        }
    };

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_template_gallery);

        if (requestPermission(Manifest.permission.READ_EXTERNAL_STORAGE)) {
            // Everything related to populating the gallery with memes
            RecyclerView recyclerView = findViewById(R.id.TemplateGallery);
            final GalleryViewAdapter adapter = new GalleryViewAdapter(this);
            recyclerView.setAdapter(adapter);
            recyclerView.setLayoutManager(new GridLayoutManager(this, 3));
            recyclerView.setHasFixedSize(true);

            // Get a new or existing ViewModel
            memeViewModel = ViewModelProviders.of(this).get(MemeViewModel.class);
            memeViewModel.getAllTemplates().observe(this, new Observer<List<MemeTemplate>>() {
                @Override
                public void onChanged(@Nullable final List<MemeTemplate> templates) {
                    for (MemeTemplate template : templates) {
                        File f = new File(template.getFilepath());
                        if (!f.exists()) {
                            memeViewModel.deleteTemplate(template);
                            return;
                        }
                    }
                    adapter.setTemplates(templates);
                }
            });
        }

        // Sets the bottom text message
        mTextMessage = findViewById(R.id.message);
        BottomNavigationView navigation = findViewById(R.id.navigation);
        navigation.setOnNavigationItemSelectedListener(mOnNavigationItemSelectedListener);

    }

    public boolean requestPermission(String permission) {
        boolean isGranted = ContextCompat.checkSelfPermission(this, permission) == PackageManager.PERMISSION_GRANTED;
        if (!isGranted) {
            ActivityCompat.requestPermissions(
                    this,
                    new String[]{permission},
                    READ_WRITE_STORAGE);
        }
        return isGranted;
    }
}