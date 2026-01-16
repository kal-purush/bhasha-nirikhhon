package com.example.bassametproject;

import androidx.annotation.NonNull;
import androidx.annotation.RequiresApi;
import androidx.appcompat.app.AppCompatActivity;
import androidx.recyclerview.widget.LinearLayoutManager;
import androidx.recyclerview.widget.RecyclerView;

import android.content.Intent;
import android.os.Build;
import android.os.Bundle;
import android.text.Editable;
import android.text.TextWatcher;
import android.view.View;
import android.widget.ArrayAdapter;
import android.widget.AutoCompleteTextView;
import android.widget.ImageView;
import android.widget.LinearLayout;
import android.widget.TextView;
import android.widget.Toast;

import com.google.firebase.database.DataSnapshot;
import com.google.firebase.database.DatabaseError;
import com.google.firebase.database.DatabaseReference;
import com.google.firebase.database.FirebaseDatabase;
import com.google.firebase.database.ValueEventListener;
import com.squareup.picasso.Picasso;

import java.util.ArrayList;
import java.util.Collections;

public class Ranking extends AppCompatActivity {
    private ArrayList<String> menus;
    private DatabaseReference refRank;
    private RecyclerView recyclerViewRank;
    private ArrayList<User> listRank;
    private rankAdapter adapterRank;
    private TextView user1name , user2name , user1point , user2point , user3name , user3point;
    private ImageView user1img , user2img , user3img;
    private AutoCompleteTextView rechercheRank , rechercheName , recherchePoint;
    private ArrayList<String> nameSearch , rankSearch , pointSearch;
    private TextView back;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_ranking);

        back = findViewById(R.id.Back);

        back.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                finish();
                Intent back = new Intent(Ranking.this, Home.class);
                startActivity(back);
            }
        });

       /* rechercheRank = findViewById(R.id.rechercheRank);
        rechercheName = findViewById(R.id.rechercheName);
        recherchePoint = findViewById(R.id.recherchePoint);*/

        user1name = (TextView) findViewById(R.id.FirstplaceName);
        user2name = (TextView) findViewById(R.id.SecondplaceName);
        user3name = (TextView) findViewById(R.id.ThirdplaceName);

        user1img = (ImageView) findViewById(R.id.FirstplaceImg);
        user2img = (ImageView) findViewById(R.id.SecondplaceImg);
        user3img = (ImageView) findViewById(R.id.ThirdplaceImg);


        recyclerViewRank = (RecyclerView) findViewById(R.id.rankingRecycler);


        recyclerViewRank.setLayoutManager(new LinearLayoutManager(this));


        listRank = new ArrayList<User>();

        //SearchView Places Names Array List
        nameSearch = new ArrayList<>();
        rankSearch = new ArrayList<>();
        pointSearch = new ArrayList<>();
        //SearchView Places Names Array List end

     //   watcherlistner();

        refRank = FirebaseDatabase.getInstance().getReference("User");
        refRank.addValueEventListener(new ValueEventListener() {
            @Override
            public void onDataChange(@NonNull DataSnapshot dataSnapshot) {

                listRank.clear();
                int i = 1;

                for (DataSnapshot dataSnapshot1 : dataSnapshot.getChildren()) {

                    User u = dataSnapshot1.child("user").getValue(User.class);

                    //SearchView Places Names Array List start
                    nameSearch.add(u.getName());
                    rankSearch.add(String.valueOf(i));
                    pointSearch.add(String.valueOf(u.getPoint()));
                    //SearchView Places Names Array List end

                    listRank.add(u);
                    i++;
                }

                Collections.sort(listRank , User.pointSort);

                Collections.reverse(listRank);

                for(int r = 0 ; r < listRank.size() ; r++)
                {
                    listRank.get(r).setRank(r+1);
                }

                Picasso.get().load(listRank.get(0).getImage()).resize(500 , 500).into(user1img);
                Picasso.get().load(listRank.get(1).getImage()).resize(500 , 500).into(user2img);
                Picasso.get().load(listRank.get(2).getImage()).resize(500 , 500).into(user3img);

                user1name.setText(listRank.get(0).getName());
                user2name.setText(listRank.get(1).getName());
                user3name.setText(listRank.get(2).getName());

                user1point.setText(String.valueOf(listRank.get(0).getPoint()));
                user2point.setText(String.valueOf(listRank.get(1).getPoint()));
                user3point.setText(String.valueOf(listRank.get(2).getPoint()));

                for(int j = 0 ; j < 3 ; j++)
                {
                    listRank.remove(0);
                }

                adapterRank = new rankAdapter( Ranking.this,listRank);
                recyclerViewRank.setAdapter(adapterRank);

                //SearchView Places Names Array List start
                ArrayAdapter<String> adapterSearch = new ArrayAdapter<String>(Ranking.this , android.R.layout.simple_list_item_1 , nameSearch);
                ArrayAdapter<String> adapterSearch2 = new ArrayAdapter<String>(Ranking.this , android.R.layout.simple_list_item_1 , rankSearch);
                ArrayAdapter<String> adapterSearch3 = new ArrayAdapter<String>(Ranking.this , android.R.layout.simple_list_item_1 , pointSearch);

                rechercheRank.setAdapter(adapterSearch2);
                rechercheName.setAdapter(adapterSearch);
                recherchePoint.setAdapter(adapterSearch3);
                //SearchView Places Names Array List end

            }

            @Override
            public void onCancelled(@NonNull DatabaseError databaseError) {
                Toast.makeText(Ranking.this, " something is wrong !", Toast.LENGTH_SHORT).show();
            }
        });



    }

    private void filterSearchName(String text)
    {

        ArrayList<User> MorePlacesSearchList = new ArrayList<>();

        for(User item : listRank)
        {
            if(item.getName().toLowerCase().contains(text.toLowerCase()))
            {
                MorePlacesSearchList.add(item);
            }
        }

        adapterRank = new rankAdapter(Ranking.this,listRank);
        recyclerViewRank.setAdapter(adapterRank);


    }
/*
    private void filterSearchRank(String text)
    {
        int i = 0;

        ArrayList<User> MorePlacesSearchList = new ArrayList<>();

        for(User item : listRank)
        {
            if(String.valueOf(item.getRank()).toLowerCase().contains(text.toLowerCase()))
            {
                MorePlacesSearchList.add(item);
            }
            i++;
        }

        adapterRank = new rankAdapter(Ranking.this,listRank);
        recyclerViewRank.setAdapter(adapterRank);


    }

    private void filterSearchPoint(String text)
    {

        ArrayList<User> MorePlacesSearchList = new ArrayList<>();

        for(User item : listRank)
        {
            if(String.valueOf(item.getPoint()).toLowerCase().contains(text.toLowerCase()))
            {
                MorePlacesSearchList.add(item);
            }
        }

        adapterRank = new rankAdapter(Ranking.this, MorePlacesSearchList);
        recyclerViewRank.setAdapter(adapterRank);


    }

    private void watcherlistner()
    {
        rechercheRank.addTextChangedListener(new TextWatcher() {
            @Override
            public void beforeTextChanged(CharSequence charSequence, int i, int i1, int i2) {

            }

            @Override
            public void onTextChanged(CharSequence charSequence, int i, int i1, int i2) {

            }

            @Override
            public void afterTextChanged(Editable s) {
                filterSearchRank(s.toString());
            }
        });

        rechercheName.addTextChangedListener(new TextWatcher() {
            @Override
            public void beforeTextChanged(CharSequence charSequence, int i, int i1, int i2) {

            }

            @Override
            public void onTextChanged(CharSequence charSequence, int i, int i1, int i2) {

            }

            @Override
            public void afterTextChanged(Editable s) {
                filterSearchName(s.toString());
            }
        });

        recherchePoint.addTextChangedListener(new TextWatcher() {
            @Override
            public void beforeTextChanged(CharSequence charSequence, int i, int i1, int i2) {

            }

            @Override
            public void onTextChanged(CharSequence charSequence, int i, int i1, int i2) {

            }

            @Override
            public void afterTextChanged(Editable s) {
                filterSearchPoint(s.toString());
            }
        });
    }
*/
    @Override
    public void onBackPressed() {
        finish();
        Intent back = new Intent(Ranking.this, Home.class);
        startActivity(back);
    }
}