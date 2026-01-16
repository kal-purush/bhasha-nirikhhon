package com.app.bestbet.bestbet;

import android.content.Intent;
import android.content.SharedPreferences;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.support.v7.widget.LinearLayoutManager;
import android.support.v7.widget.RecyclerView;
import android.view.Menu;
import android.view.MenuItem;
import android.view.View;
import android.widget.TextView;
import android.widget.Toast;

import java.util.ArrayList;

public class ActiveBets extends AppCompatActivity {


    private RecyclerView betsRecyclerView;
    private RecyclerView.Adapter betsAdapter;
    private SharedPreferences savedValues;

    private ArrayList<Bet> bets;
    private ArrayList<Person> people;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_active_bets);

        //Gets db object
        BestBetDB db = new BestBetDB(this);

        betsRecyclerView = (RecyclerView) findViewById(R.id.betRecyclerView);

        //Sets layout manager on recyclerview
        LinearLayoutManager llm = new LinearLayoutManager(this);
        betsRecyclerView.setLayoutManager(llm);

        //Gets list of players
        bets = db.getActiveBets();
        people = db.getPeople();

        //Sets adapter on the recyclerview
        betsAdapter = new RecyclerViewAdapter(bets, people);
        betsRecyclerView.setAdapter(betsAdapter);

        //Adds an on click listener to each recyclerview
        betsRecyclerView.addOnItemTouchListener(
                new RecyclerItemClickListener(this, new RecyclerItemClickListener.OnItemClickListener() {
                    @Override
                    public void onItemClick(View view, int position) {
                        savedValues = getSharedPreferences("SavedValues", MODE_PRIVATE);

                        SharedPreferences.Editor editor = savedValues.edit();
                        String betId = ((TextView) view.findViewById(R.id.betId)).getText().toString();

                        editor.putString("betID", betId);

                        Toast.makeText(getBaseContext(), betId, Toast.LENGTH_SHORT).show();

                        editor.commit();
                        startActivity(new Intent(getApplicationContext(), BetDetails.class));
                    }
                })
        );

    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.menu_active_bets, menu);
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