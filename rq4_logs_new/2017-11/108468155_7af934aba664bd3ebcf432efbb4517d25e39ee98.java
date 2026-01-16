package aaron.filecommand.activity;

import android.os.Bundle;
import android.support.v7.app.AppCompatActivity;
import android.support.v7.widget.CardView;
import android.support.v7.widget.LinearLayoutManager;
import android.support.v7.widget.RecyclerView;
import android.support.v7.widget.Toolbar;
import android.util.Log;
import android.widget.ImageView;
import android.widget.TextView;
import android.widget.Toast;

import java.util.ArrayList;
import java.util.HashMap;

import aaron.filecommand.R;
import aaron.filecommand.dao.Category;
import aaron.filecommand.dao.CategoryRepo;

/**
 * Created by Yunwen on 10/30/2017.
 */

public class AddCategoryActivity extends AppCompatActivity {
    private Toolbar toolbar;
    private TextView toolbarTitle, textTitle, textContent;
    private ImageView imageToolbar;
    private CardView cardView;

    private RecyclerView mRecyclerView;
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_addcategory);
        initView();
    }

    private void initView(){
        toolbar = (Toolbar) findViewById(R.id.toolbar);
        setSupportActionBar(toolbar);
        getSupportActionBar().setDisplayHomeAsUpEnabled(true);
        getSupportActionBar().setDisplayShowHomeEnabled(true);
        toolbarTitle = (TextView) findViewById(R.id.text_toolbar_title);
        setToolbarHome();
    }

    private void setToolbarHome(){
        toolbarTitle.setText("Add Category");
    }

    @Override
    public boolean onSupportNavigateUp() {
        onBackPressed();
        return true;
    }
    public void listAll() {
        int _algorithm_id = 0;
        CategoryRepo repo = new CategoryRepo(this);
        Category category = new Category();
        Log.d("MainActivity", "The id is: " + _algorithm_id);
        category = repo.getColumnById(_algorithm_id);
        ArrayList<HashMap<String, String>> algorithmList = repo.getAlgorithmList();
        if (algorithmList.size() != 0) {//Show Db list
            initRecyclerView(algorithmList);
        } else {
            Toast.makeText(this, "No Content!", Toast.LENGTH_SHORT).show();
        }
    }

    private void initRecyclerView(ArrayList<HashMap<String, String>> algorithmList){
//        mRecyclerView = (RecyclerView) findViewById(R.id.recyclerview_db);
//        mRecyclerView.setLayoutManager(new LinearLayoutManager(this));
//        dbAdapter = new DbAdapter(this,algorithmList);
//        mRecyclerView.setAdapter(dbAdapter);
    }
}