package com.example.planties.features.plant_care.garden.detail.adapter.garden;

import android.view.LayoutInflater;
import android.view.ViewGroup;

import androidx.annotation.NonNull;
import androidx.recyclerview.widget.ListAdapter;

import com.example.planties.databinding.ItemGardenBinding;
import com.example.planties.databinding.ItemPhotosBinding;
import com.example.planties.domain.garden.model.GardenModel;

public class GardenAdapter extends ListAdapter<GardenPhotosModel, GardenViewHolder> {

    public GardenAdapter() {
        super(new GardenDiffUtil());
    }

    @NonNull
    @Override
    public GardenViewHolder onCreateViewHolder(@NonNull ViewGroup parent, int viewType) {
        ItemPhotosBinding binding = ItemPhotosBinding.inflate(LayoutInflater.from(parent.getContext()), parent, false);
        return new GardenViewHolder(binding);
    }

    @Override
    public void onBindViewHolder(@NonNull GardenViewHolder holder, int position) {
        holder.bind(getItem(position));
    }
}