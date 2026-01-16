package com.hyep.movietracker.adapter;

import android.content.Context;
import android.view.ViewGroup;

import androidx.annotation.NonNull;
import androidx.recyclerview.widget.RecyclerView;

import com.hyep.movietracker.models.PersonalSpace;

import java.util.List;

public class PersonalSpaceAdapter extends RecyclerView.Adapter{

    private Context context;

    private List<PersonalSpace> personalSpaceList;

    @NonNull
    @Override
    public RecyclerView.ViewHolder onCreateViewHolder(@NonNull ViewGroup parent, int viewType) {
        return null;
    }

    @Override
    public void onBindViewHolder(@NonNull RecyclerView.ViewHolder holder, int position) {

    }

    @Override
    public int getItemCount() {
        return 0;
    }

}