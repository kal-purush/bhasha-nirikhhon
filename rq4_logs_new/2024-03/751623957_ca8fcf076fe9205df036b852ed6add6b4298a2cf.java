package com.example.its_a_feature_not_a_bug;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.TextView;
import androidx.annotation.NonNull;
import androidx.recyclerview.widget.RecyclerView;
import java.util.ArrayList;

public class AttendeeAdapter extends RecyclerView.Adapter<AttendeeAdapter.AttendeeViewHolder> {

    private ArrayList<String> attendees;

    public AttendeeAdapter(ArrayList<String> attendees) {
        if (attendees == null) {
            this.attendees = new ArrayList<>();
            // populate with dummy data
            this.attendees.add("Jing ");
            this.attendees.add("Tanveer");

        } else {
            this.attendees = attendees;
        }
    }


    @NonNull
    @Override
    public AttendeeViewHolder onCreateViewHolder(@NonNull ViewGroup parent, int viewType) {
        View view = LayoutInflater.from(parent.getContext()).inflate(android.R.layout.simple_list_item_1, parent, false);
        return new AttendeeViewHolder(view);
    }

    @Override
    public void onBindViewHolder(@NonNull AttendeeViewHolder holder, int position) {
        holder.attendeeName.setText(attendees.get(position));
    }

    @Override
    public int getItemCount() {
        return attendees.size();
    }

    static class AttendeeViewHolder extends RecyclerView.ViewHolder {
        TextView attendeeName;

        AttendeeViewHolder(View itemView) {
            super(itemView);
            attendeeName = itemView.findViewById(android.R.id.text1);
        }
    }
}
