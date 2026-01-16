package com.example.planties.features.oxygen_leaderboard.adapter;

import androidx.annotation.NonNull;
import androidx.recyclerview.widget.DiffUtil;

import com.example.planties.data.leaderboards.remote.dto.LeaderboardsResModel;

public class LeaderboardDiffUtill extends DiffUtil.ItemCallback<LeaderboardsResModel> {

    @Override
    public boolean areItemsTheSame(@NonNull LeaderboardsResModel oldItem, @NonNull LeaderboardsResModel newItem) {
        return oldItem.getUserId().equals(newItem.getUserId());
    }

    @Override
    public boolean areContentsTheSame(@NonNull LeaderboardsResModel oldItem, @NonNull LeaderboardsResModel newItem) {
        return oldItem.getUsername().equals(newItem.getUsername()) &&
                oldItem.getOxygen() == newItem.getOxygen() &&
                oldItem.getRank() == newItem.getRank();
    }
}