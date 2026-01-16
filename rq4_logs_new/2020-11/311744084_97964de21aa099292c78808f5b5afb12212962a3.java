package com.partnersincrime.foxdarkmaster.geekeventsmobileapp.Adapters;

import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.TextView;

import androidx.recyclerview.widget.RecyclerView;
import com.partnersincrime.foxdarkmaster.geekeventsmobileapp.R;


public class InfoTicketAdapter extends RecyclerView.Adapter<InfoTicketAdapter.ViewHolder>  {

    private String[] TicketData;

    public static class ViewHolder extends RecyclerView.ViewHolder {
        private final TextView textView;

        public ViewHolder(View view) {
            super(view);
            // Define click listener for the ViewHolder's View

            textView = (TextView) view.findViewById(R.id.textView);
        }

        public TextView getTextView() {
            return textView;
        }
    }

    public InfoTicketAdapter(String[] dataSet) {
        TicketData = dataSet;
    }

    @Override
    public ViewHolder onCreateViewHolder(ViewGroup viewGroup, int viewType) {
        View view = LayoutInflater.from(viewGroup.getContext())
                .inflate(R.layout.card_ticket_type, viewGroup, false);

        return new ViewHolder(view);
    }

    @Override
    public void onBindViewHolder(ViewHolder viewHolder, final int position) {


        viewHolder.getTextView().setText(TicketData[position]);
    }

    @Override
    public int getItemCount() {
        return TicketData.length;
    }
}