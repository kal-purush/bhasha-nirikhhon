package it.arduin.tables;

import android.content.Intent;
import android.provider.MediaStore;
import android.support.v7.app.ActionBarActivity;
import android.os.Bundle;
import android.support.v7.widget.LinearLayoutManager;
import android.support.v7.widget.RecyclerView;
import android.support.v7.widget.Toolbar;
import android.view.Menu;
import android.view.MenuItem;
import android.widget.Button;
import android.widget.Toast;

import com.melnykov.fab.FloatingActionButton;

import java.io.File;
import java.net.URLConnection;
import java.text.SimpleDateFormat;
import java.util.Date;

import butterknife.InjectView;
import butterknife.OnClick;


public class DatabaseViewInfoActivity extends BaseProjectActivity {
    @InjectView(R.id.list)
    RecyclerView mRecyclerView;
    private SimpleTextAdapter mAdapter;
    @InjectView(R.id.fab)
    FloatingActionButton fab;
    Toolbar toolbar;
    DatabaseHolder dbh;
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_database_view_info);
        if(savedInstanceState==null) {
            Intent i = getIntent();
            dbh=i.getParcelableExtra("db");
        }
        toolbar = ToolbarUtils.getSettedToolbar(this,R.id.toolbar);
        setSupportActionBar(toolbar);
        getSupportActionBar().setDisplayHomeAsUpEnabled(true);
        toolbar.setTitle(dbh.getName());
        mRecyclerView.setLayoutManager(new LinearLayoutManager(this));
        mAdapter = new SimpleTextAdapter();
        mRecyclerView.setAdapter(mAdapter);
        loadInfo();
    }

    public void loadInfo(){
        mAdapter.add(getString(R.string.DatabaseViewInfoActivity_filepath), dbh.getPath());
        File f=new File(dbh.getPath());
        mAdapter.add(getString(R.string.DatabaseViewInfoActivity_filesize), f.length() + " bytes");
        mAdapter.add(getString(R.string.DatabaseViewInfoActivity_filesize), (int) (f.length() / 1024) + " KB");
        mAdapter.add(getString(R.string.DatabaseViewInfoActivity_tables), dbh.getTableNumber() + "");
        mAdapter.add(getString(R.string.DatabaseViewInfoActivity_indexes),dbh.getIndexNumber()+"");
        mAdapter.add(getString(R.string.DatabaseViewInfoActivity_hashcode), f.hashCode() + "");
        SimpleDateFormat s= new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
        mAdapter.add(getString(R.string.DatabaseViewInfoActivity_lastedit), s.format(f.lastModified()));
    }


    public void showDeletePopup(){
        Toast.makeText(this,"dassd",Toast.LENGTH_LONG).show();
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.menu_database_view_info, menu);
        return true;
    }

    @Override
    public boolean onOptionsItemSelected(MenuItem item) {
        // Handle action bar item clicks here. The action bar will
        // automatically handle clicks on the Home/Up button, so long
        // as you specify a parent activity in AndroidManifest.xml.
        int id = item.getItemId();

        //noinspection SimplifiableIfStatement
        if (id == R.id.action_settings) {
            return true;
        }

        return super.onOptionsItemSelected(item);
    }
}