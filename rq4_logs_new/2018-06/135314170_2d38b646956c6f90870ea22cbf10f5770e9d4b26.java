package com.example.mohammad.studentpa;

import android.content.Context;
import android.content.Intent;
import android.support.annotation.NonNull;
import android.support.v7.widget.RecyclerView;
import android.util.Log;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.LinearLayout;
import android.widget.TextView;
import android.widget.Toast;

import java.util.ArrayList;

public class RecyclerViewAdapter extends RecyclerView.Adapter<RecyclerViewAdapter.ViewHolder> {
    private static final String TAG = "RecyclerViewAdapter";

    private ArrayList<String> titleNames;
    private ArrayList<String> notes;
    private Context context;

    public RecyclerViewAdapter(Context context, ArrayList<String> titleNames, ArrayList<String> notes) {
        this.titleNames = titleNames;
        this.notes = notes;
        this.context = context;
    }
    @NonNull
    @Override
    public ViewHolder onCreateViewHolder(@NonNull ViewGroup parent, int viewType) {
        View view= LayoutInflater.from(parent.getContext())
                .inflate(R.layout.recyclerview_note_display, parent, false);
        ViewHolder holder= new ViewHolder(view);
        return holder;
    }

    @Override
    public void onBindViewHolder(@NonNull ViewHolder holder, final int position) {
        Log.d(TAG, "onBindViewHolder: called.");//log tag

        holder.textViewTitle.setText(titleNames.get(position));
        holder.textViewMilestone.setText(notes.get(position));

        holder.milestoneLayout.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                //On item click, start note taker activity
                Intent noteIntent= new Intent(context, TakeMilestoneNote.class);
                //TakeMilestoneNote class needs to know WHICH note is being accessed
                noteIntent.putExtra("note_title", titleNames.get(position));
                noteIntent.putExtra("note_details", notes.get(position));
                context.startActivity(noteIntent);

                Toast.makeText(context, notes.get(position), Toast.LENGTH_SHORT).show();

            }
        });

    }

    @Override
    public int getItemCount() {
        return titleNames.size();
    }

    public class ViewHolder extends RecyclerView.ViewHolder{

        TextView textViewTitle;
        TextView textViewMilestone;
        LinearLayout milestoneLayout;

        public ViewHolder(View itemView) {
            super(itemView);
            textViewTitle = itemView.findViewById(R.id.textViewTitle);
            textViewMilestone = itemView.findViewById(R.id.textViewMilestone);
            milestoneLayout = itemView.findViewById(R.id.recyclerview_note_display);
        }
    }
}