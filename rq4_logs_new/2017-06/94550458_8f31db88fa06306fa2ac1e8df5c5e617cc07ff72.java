package org.jamaica.pcog.mobile.about;

import android.content.Intent;
import android.os.Bundle;
import android.support.v7.app.AppCompatActivity;
import android.support.v7.widget.Toolbar;
import android.view.View;
import android.widget.AdapterView;
import android.widget.ImageButton;
import android.widget.ListView;

import org.jamaica.pcog.mobile.MainActivity;
import org.jamaica.pcog.mobile.R;
import org.jamaica.pcog.mobile.about.activity.MissionActivity;
import org.jamaica.pcog.mobile.about.activity.VisionActivity;
import org.jamaica.pcog.mobile.more.MapsActivity;

import java.util.ArrayList;
import java.util.List;



public class AboutActivity extends AppCompatActivity implements AdapterView.OnItemClickListener {

    private List<AboutItem> mAboutItems = new ArrayList<AboutItem>();

    ImageButton img;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_about);
        Toolbar mToolbar = (Toolbar) findViewById(R.id.toolbar);

        img = (ImageButton)findViewById(R.id.backbtn);

        if (mToolbar != null) {

            setSupportActionBar(mToolbar);

            img.setOnClickListener(new View.OnClickListener() {
                @Override
                public void onClick(View v) {
                    Intent intent = new Intent(getApplicationContext(), MainActivity.class);
                    startActivity(intent);
                    finish();
                }
            });

        }
        getSupportActionBar().setTitle("About Us");

        ListView mListView = (ListView) findViewById(R.id.listView);
        mListView.setOnItemClickListener(this);
        generateList();
        AboutAdapter mAdapter = new AboutAdapter(this, 0x00, mAboutItems);
        mListView.setAdapter(mAdapter);
    }

    private void generateList() {
        mAboutItems.clear();
        mAboutItems.add(new AboutItem(getString(R.string.vision),
                getString(R.string.vision_text),
                R.drawable.arrow1));
        mAboutItems.add(new AboutItem(getString(R.string.mission),
                getString(R.string.mission_text),
                R.drawable.arrow1));
        mAboutItems.add(new AboutItem(getString(R.string.local),
                getString(R.string.location_text),
                R.drawable.arrow1));


    }

    @Override
    public void onItemClick(AdapterView<?> parent, View view, int position, long id) {
        switch (position) {
            case 0: //Launch Navigation to the conference site
                Intent i = new Intent(getApplicationContext(),VisionActivity.class);
                finish();
                startActivity(i);
                break;
            case 1: // Display the Open Source projects used for this application
                Intent i1 = new Intent(getApplicationContext(),MissionActivity.class);
                finish();
                startActivity(i1);
                break;
            case 2: // Display the Open Source projects used for this application
                Intent i2 = new Intent(getApplicationContext(),MapsActivity.class);
                finish();
                startActivity(i2);
                break;

        }
    }
    @Override
    public void onBackPressed() {
        Intent intent = new Intent(getApplicationContext(), MainActivity.class);
        startActivity(intent);
        overridePendingTransition(R.anim.fade_in, R.anim.fade_out);
        this.finish();
    }
}