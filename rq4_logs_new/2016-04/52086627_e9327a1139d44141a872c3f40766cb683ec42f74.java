package br.edu.ifce.engcomp.francis.diversidade.activities;

import android.content.Intent;
import android.os.Bundle;
import android.support.design.widget.FloatingActionButton;
import android.support.design.widget.Snackbar;
import android.support.v7.app.AppCompatActivity;
import android.support.v7.widget.DefaultItemAnimator;
import android.support.v7.widget.LinearLayoutManager;
import android.support.v7.widget.RecyclerView;
import android.support.v7.widget.Toolbar;
import android.view.View;

import java.util.ArrayList;

import br.edu.ifce.engcomp.francis.diversidade.R;
import br.edu.ifce.engcomp.francis.diversidade.adapters.DetailDeveloperRecyclerViewAdapter;
import br.edu.ifce.engcomp.francis.diversidade.adapters.TextRecyclerViewAdapter;
import br.edu.ifce.engcomp.francis.diversidade.interfaces.RecyclerViewOnClickListenerHack;
import br.edu.ifce.engcomp.francis.diversidade.model.Contact;
import br.edu.ifce.engcomp.francis.diversidade.model.Developer;
import br.edu.ifce.engcomp.francis.diversidade.model.TextBlog;

public class DetailDeveloperActivity extends AppCompatActivity implements RecyclerViewOnClickListenerHack {
    RecyclerView recyclerView;
    LinearLayoutManager layoutManager;
    ArrayList<Contact> dataSource;
    Developer developer;

    public void initDataSource() {
        Intent currentIntent = getIntent();
        developer = (Developer) currentIntent.getSerializableExtra("INFOS_DEVELOPER");
        this.dataSource = developer.getContacts();
    }

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_detail_developer);

        initDataSource();

        Toolbar toolbar = (Toolbar) findViewById(R.id.toolbar);
        if (toolbar != null) {
            setSupportActionBar(toolbar);
            setTitle(developer.getName());
            getSupportActionBar().setDisplayHomeAsUpEnabled(true);
            getSupportActionBar().setDisplayShowHomeEnabled(true);
        }

        DetailDeveloperRecyclerViewAdapter adapter = new DetailDeveloperRecyclerViewAdapter(dataSource, getApplicationContext());
        adapter.setRecycleViewOnClickListenerHack(this);
        this.layoutManager = new LinearLayoutManager(this);
        this.recyclerView = (RecyclerView) findViewById(R.id.dev_infos_recyler_view);

        this.recyclerView.setHasFixedSize(false);
        this.recyclerView.setLayoutManager(layoutManager);
        this.recyclerView.setAdapter(adapter);
        this.recyclerView.setItemAnimator(new DefaultItemAnimator());

    }

    @Override
    public void onClickListener(View view, int position) {

    }
}