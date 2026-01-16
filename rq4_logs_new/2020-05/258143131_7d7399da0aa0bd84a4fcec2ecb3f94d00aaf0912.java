package com.example.premierleagueapp.adapter;

import android.annotation.SuppressLint;
import android.app.Activity;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ImageView;
import android.widget.RelativeLayout;
import android.widget.TextView;

import androidx.annotation.NonNull;
import androidx.recyclerview.widget.LinearLayoutManager;
import androidx.recyclerview.widget.RecyclerView;

import com.example.premierleagueapp.R;
import com.example.premierleagueapp.model.AwayTeam;
import com.example.premierleagueapp.model.FixturesByMatchday;
import com.example.premierleagueapp.model.FullTime;
import com.example.premierleagueapp.model.HomeTeam;
import com.example.premierleagueapp.model.Match;
import com.example.premierleagueapp.model.Matches;
import com.example.premierleagueapp.model.Score;

import java.util.ArrayList;
import java.util.HashMap;

public class RecyclerViewAdapterMatchdays extends RecyclerView.Adapter<RecyclerViewAdapterMatchdays.ViewHolder>{
    private ArrayList<Integer> matchdays;
    private ArrayList<Match> matches;
    private Activity activity;

    public RecyclerViewAdapterMatchdays(ArrayList<Integer> matchdays, ArrayList<Match> matches,  Activity activity) {
        this.matchdays = matchdays;
        this.matches = matches;
        this.activity = activity;
    }

    @NonNull
    @Override
    public ViewHolder onCreateViewHolder(@NonNull ViewGroup parent, int viewType) {
        View view = LayoutInflater.from(parent.getContext()).inflate(R.layout.matchday_item, parent, false);
        return new ViewHolder(view);
    }

    @SuppressLint("SetTextI18n")
    @Override
    public void onBindViewHolder(@NonNull ViewHolder holder, int position) {
        holder.matchday.setText("Matchday " + matchdays.get(position));

//        for(int i= 0; i < 5; i++) {
//            HomeTeam homeTeam = new HomeTeam(i+1, "lalla");
//            AwayTeam awayTeam = new AwayTeam(i+1, "lalla");
//            FullTime fullTime = new FullTime(1, 2);
//            Score score = new Score(null, null, fullTime, null, null, null);
//            matches.add(new Match(i, null, null , null, i, null ,null, null, null, score, homeTeam, awayTeam, null));
//        }
        ArrayList<Match> matchesByMatchday = new ArrayList<>();

        for(Match match : matches) {
            if(match.getMatchday() == 1) {
                matchesByMatchday.add(match);
            }
        }

        RecyclerViewAdapterFixtures recyclerViewAdapterFixtures = new RecyclerViewAdapterFixtures(matchesByMatchday, matchdays);
        LinearLayoutManager linearLayoutManager = new LinearLayoutManager(activity);
        holder.recyclerView.setLayoutManager(linearLayoutManager);
        holder.recyclerView.setAdapter(recyclerViewAdapterFixtures);
    }

    @Override
    public int getItemCount() {
        return matchdays.size();
    }

    public class ViewHolder extends RecyclerView.ViewHolder {

        TextView matchday;
        RecyclerView recyclerView;

        public ViewHolder(@NonNull View itemView) {
            super(itemView);
            matchday = itemView.findViewById(R.id.matchdayNumber);
            recyclerView = itemView.findViewById(R.id.recycler_view_matchday);

        }
    }


}