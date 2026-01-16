package com.example.myapplication.adapters;

import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ImageView;
import android.widget.TextView;

import androidx.annotation.NonNull;
import androidx.recyclerview.widget.RecyclerView;

import com.example.myapplication.R;
import com.example.myapplication.models.History;

import java.util.List;

public class HistoryAdapter extends RecyclerView.Adapter<HistoryAdapter.HistoryViewHolder> {
    List<History> histories;

    public HistoryAdapter(List<History> histories) {
        this.histories = histories;
    }

    @NonNull
    @Override
    public HistoryViewHolder onCreateViewHolder(@NonNull ViewGroup parent, int viewType) {
        View listItemView = LayoutInflater.from(parent.getContext())
                .inflate(R.layout.history_item, parent, false);
        return new HistoryViewHolder(listItemView);
    }

    @Override
    public void onBindViewHolder(@NonNull HistoryViewHolder holder, int position) {
        holder.image.setImageResource(histories.get(position).getImageResRef());
        holder.description.setText(histories.get(position).getDescription());
        holder.date.setText(histories.get(position).getDate());
        holder.price.setText(histories.get(position).getPrice());
    }

    @Override
    public int getItemCount() {
        return histories.size();
    }

    public static class HistoryViewHolder extends RecyclerView.ViewHolder {

        private ImageView image;
        private TextView description;
        private TextView date;
        private TextView price;

        public HistoryViewHolder(@NonNull View itemView) {
            super(itemView);
            image = itemView.findViewById(R.id.history_item_image);
            description = itemView.findViewById(R.id.history_item_description);
            date = itemView.findViewById(R.id.history_item_date);
            price = itemView.findViewById(R.id.history_item_price);
        }
    }
}