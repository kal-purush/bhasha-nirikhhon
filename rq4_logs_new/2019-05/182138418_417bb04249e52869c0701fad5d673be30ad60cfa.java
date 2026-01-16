package ru.nbdev.androidapp3;

import androidx.appcompat.app.AppCompatActivity;
import androidx.recyclerview.widget.LinearLayoutManager;
import androidx.recyclerview.widget.RecyclerView;

import android.os.Bundle;

import java.util.ArrayList;
import java.util.List;

public class RecyclerActivity extends AppCompatActivity {

    private RecyclerView recyclerView;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_recycler);

        recyclerInit();
    }

    private void recyclerInit() {
        recyclerView = findViewById(R.id.recycler_view);

        RecyclerView.LayoutManager layoutManager = new LinearLayoutManager(this);
        recyclerView.setLayoutManager(layoutManager);

        List<Integer> imagesList = new ArrayList<>();
        imagesList.add(R.drawable.fruits);
        imagesList.add(R.drawable.vegetables);
        imagesList.add(R.drawable.nature);
        imagesList.add(R.drawable.fruits);
        imagesList.add(R.drawable.vegetables);
        imagesList.add(R.drawable.nature);

        RecyclerView.Adapter adapter = new RecyclerAdapter(imagesList);
        recyclerView.setAdapter(adapter);

        recyclerView.addItemDecoration(new SpacesItemDecorator(50));
    }
}