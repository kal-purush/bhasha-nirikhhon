package com.dtavana.foodswipe.adapters;

import android.content.Context;
import android.util.Log;
import android.view.LayoutInflater;
import android.view.ViewGroup;
import android.widget.CompoundButton;

import androidx.annotation.NonNull;
import androidx.recyclerview.widget.RecyclerView;

import com.dtavana.foodswipe.databinding.ItemFilterBinding;
import com.dtavana.foodswipe.models.RestaurantFilter;

import java.util.List;

public class FiltersAdapter extends RecyclerView.Adapter<FiltersAdapter.ViewHolder> {
    public static final String TAG = "FilterAdapter";

    Context ctx;
    List<RestaurantFilter> filters;

    public FiltersAdapter(Context context, List<RestaurantFilter> filters) {
        this.ctx = context;
        this.filters = filters;
    }

    public void clear() {
        filters.clear();
        notifyDataSetChanged();
    }

    @NonNull
    @Override
    public ViewHolder onCreateViewHolder(@NonNull ViewGroup parent, int viewType) {
        ItemFilterBinding binding = ItemFilterBinding.inflate(LayoutInflater.from(ctx), parent, false);
        return new ViewHolder(binding);
    }

    @Override
    public void onBindViewHolder(@NonNull ViewHolder holder, int position) {
        RestaurantFilter filter = filters.get(position);
        holder.bind(filter);
    }

    @Override
    public int getItemCount() {
        return filters.size();
    }

    static class ViewHolder extends RecyclerView.ViewHolder {

        ItemFilterBinding binding;

        RestaurantFilter filter;

        public ViewHolder(ItemFilterBinding binding) {
            super(binding.getRoot());
            this.binding = binding;
        }

        public void bind(final RestaurantFilter filter) {
            this.filter = filter;
            binding.cbItem.setText(filter.getName());
            binding.cbItem.setOnCheckedChangeListener(new CompoundButton.OnCheckedChangeListener() {
                @Override
                public void onCheckedChanged(CompoundButton compoundButton, boolean b) {
                    Log.d(TAG, "onCheckedChanged: Check changed");
                    filter.setChecked(b);
                }
            });
        }
    }
}