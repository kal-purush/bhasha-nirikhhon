package com.group4sweng.scranplan;

import android.content.Intent;
import android.os.Bundle;
import android.view.View;

import androidx.appcompat.app.AppCompatActivity;
import androidx.recyclerview.widget.LinearLayoutManager;
import androidx.recyclerview.widget.RecyclerView;

import java.util.ArrayList;


public class savedList extends AppCompatActivity implements RecyclerViewAdaptor.ItemClickListener  {
    final String TAG = "SavedShoppingList";


    //User information
    private com.group4sweng.scranplan.UserInfo.UserInfoPrivate mUser;

    RecyclerViewAdaptor adapter;
    ArrayList<String> newList3 = new ArrayList<>();

    @Override
    protected void onCreate(Bundle savedInstanceState) {


        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_savedshoppinglist);

        mUser = (com.group4sweng.scranplan.UserInfo.UserInfoPrivate) getIntent().getSerializableExtra("user");
        Intent i = getIntent();
        newList3 = i.getStringArrayListExtra("newList3");

        if (mUser != null) {
            RecyclerView recyclerView = findViewById(R.id.ShoppingList);
            recyclerView.setLayoutManager(new LinearLayoutManager(this));
            adapter = new RecyclerViewAdaptor(this, newList3);
            adapter.setClickListener(this);
            recyclerView.setAdapter(adapter);
        }

    }

    @Override
    public void onItemClick(View view, int position) {

        if (newList3 != null) {
            newList3.remove(position);
        }
        adapter.notifyItemRemoved(position);

    }


}