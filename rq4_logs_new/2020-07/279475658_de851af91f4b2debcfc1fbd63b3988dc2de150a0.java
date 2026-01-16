package com.example.cst2335.soccer;

import android.os.Bundle;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ListView;

import androidx.annotation.NonNull;
import androidx.annotation.Nullable;
import androidx.fragment.app.Fragment;

import com.example.cst2335.R;

import java.util.ArrayList;

public class FavoriteMatchesFragment extends Fragment {

    private View itemView;
    private ArrayList<SoccerMatch> allMatches;
    private MatchAdapter matchAdapter;
    private ListView soccerMatchList;

    @Nullable
    @Override
    public View onCreateView(@NonNull LayoutInflater inflater, @Nullable ViewGroup container, @Nullable Bundle savedInstanceState) {
        itemView = inflater.inflate(R.layout.item_match, container, false);
        soccerMatchList = itemView.findViewById(R.id.soccerMatchList);
        allMatches = new ArrayList<>();
        matchAdapter = new MatchAdapter(getActivity(), allMatches);
        soccerMatchList.setAdapter(matchAdapter);
        return itemView;
    }
}