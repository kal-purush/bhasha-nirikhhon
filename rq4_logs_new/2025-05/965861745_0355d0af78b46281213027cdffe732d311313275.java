package com.daclink.citypulse;

import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.TextView;

import androidx.annotation.NonNull;
import androidx.recyclerview.widget.RecyclerView;

import com.daclink.citypulse.model.EventItem;

import java.util.List;

public class EventItemAdapter extends RecyclerView.Adapter<EventItemAdapter.EventViewHolder> {

    private final List<EventItem> events;

    public EventItemAdapter(List<EventItem> events) {
        this.events = events;
    }

    @NonNull
    @Override
    public EventViewHolder onCreateViewHolder(@NonNull ViewGroup parent, int viewType) {
        View view = LayoutInflater.from(parent.getContext())
                .inflate(R.layout.item_event, parent, false);
        return new EventViewHolder(view);
    }

    @Override
    public void onBindViewHolder(@NonNull EventViewHolder holder, int position) {
        EventItem event = events.get(position);
        holder.titleTextView.setText(event.getName());
        holder.venueTextView.setText(event.getVenueName());
        holder.dateTextView.setText(event.getLocalDate());
    }

    @Override
    public int getItemCount() {
        return events.size();
    }

    static class EventViewHolder extends RecyclerView.ViewHolder {
        TextView titleTextView, venueTextView, dateTextView;

        EventViewHolder(View itemView) {
            super(itemView);
            titleTextView = itemView.findViewById(R.id.eventTitle);
            venueTextView = itemView.findViewById(R.id.eventLocation);
            dateTextView = itemView.findViewById(R.id.eventDate);
        }
    }
}