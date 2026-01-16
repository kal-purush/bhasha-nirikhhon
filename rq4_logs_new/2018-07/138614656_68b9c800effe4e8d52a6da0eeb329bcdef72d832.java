package com.andela.javadevelopers;

import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.support.v7.widget.GridLayoutManager;
import android.support.v7.widget.RecyclerView;

import java.util.ArrayList;
import java.util.List;

public class MainActivity extends AppCompatActivity {

    private RecyclerView recyclerView;
    private RecyclerView.Adapter adapter;

    private static List<ListItem> listItems = new ArrayList<>();

    // dummy static data object
    static {
        listItems.add(new ListItem("https://avatars1.githubusercontent.com/u/22524619?s=400&v=4", "Moana Jones"));
        listItems.add(new ListItem("https://avatars2.githubusercontent.com/u/15001016?v=4", "Johnny Donny"));
        listItems.add(new ListItem("https://avatars0.githubusercontent.com/u/30238960?s=400&v=4", "Leumas Sam"));
        listItems.add(new ListItem("https://avatars1.githubusercontent.com/u/4929406?v=4", "Ceasar Clown"));
        listItems.add(new ListItem("https://avatars0.githubusercontent.com/u/4760130?v=4", "Jackie Chan"));
        listItems.add(new ListItem("https://avatars0.githubusercontent.com/u/4107815?v=4", "Tinnie Tempa"));

    }


    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);

        recyclerView = findViewById(R.id.recycler_view);
        recyclerView.setHasFixedSize(true);
        recyclerView.setLayoutManager(new GridLayoutManager(this, 2));

        adapter = new DevListAdapter(listItems, this);
        recyclerView.setAdapter(adapter);
    }
}