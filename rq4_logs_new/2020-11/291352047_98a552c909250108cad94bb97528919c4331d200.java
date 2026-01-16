package com.operationcodify.cavoid.activities;

import android.content.Context;
import android.view.LayoutInflater;
import android.view.ViewGroup;

import androidx.annotation.NonNull;
import androidx.cardview.widget.CardView;
import androidx.recyclerview.widget.RecyclerView;
import androidx.recyclerview.widget.SortedList;

import com.operationcodify.cavoid.R;

import java.util.List;

public class PastLocationAdapter extends RecyclerView.Adapter<PastLocationViewHolder>{
    PastLocationViewHolder vh;
    SortedList<ParsedPastLocationReport> pastLocationReports;
    Context c;

    //The adapter constructor
    public PastLocationAdapter(Context c, List<ParsedPastLocationReport> data) {
        this.c = c;
        pastLocationReports = new SortedList<>(ParsedPastLocationReport.class, new SortedList.Callback<ParsedPastLocationReport>() {
            @Override
            public int compare(ParsedPastLocationReport o1, ParsedPastLocationReport o2) {
                if (o1.reportGenerationDate == null){
                    return 1;
                }
                else if (o2.reportGenerationDate == null){
                    return -1;
                }
                else{
                    return o1.reportGenerationDate.compareTo(o2.reportGenerationDate);
                }
            }

            @Override
            public void onChanged(int position, int count) {
                notifyItemRangeChanged(position, count);
            }

            @Override
            public boolean areContentsTheSame(ParsedPastLocationReport oldItem, ParsedPastLocationReport newItem) {
                return oldItem.fips.equals(newItem.fips) && oldItem.hashCode() == newItem.hashCode();
            }

            @Override
            public boolean areItemsTheSame(ParsedPastLocationReport item1, ParsedPastLocationReport item2) {
                return item1.fips.equals(item2.fips);
            }

            @Override
            public void onInserted(int position, int count) {
                notifyItemRangeChanged(position, count);
            }

            @Override
            public void onRemoved(int position, int count) {
                notifyItemRangeRemoved(position, count);
            }

            @Override
            public void onMoved(int fromPosition, int toPosition) {
                notifyItemMoved(fromPosition, toPosition);
            }
        });
        addAll(data);
    }

    public void addAll(List<ParsedPastLocationReport> reports){
        pastLocationReports.beginBatchedUpdates();
        for (int i = 0; i < reports.size(); i++){
            pastLocationReports.add(reports.get(i));
        }
        pastLocationReports.endBatchedUpdates();
    }

    public void add(ParsedPastLocationReport report){
        pastLocationReports.add(report);
        notifyDataSetChanged();
    }

    public ParsedPastLocationReport get(int position) {
        return pastLocationReports.get(position);
    }

    public void clear(){
        pastLocationReports.beginBatchedUpdates();
        while (pastLocationReports.size() > 0){
            pastLocationReports.removeItemAt(pastLocationReports.size()-1);
        }
        pastLocationReports.endBatchedUpdates();
    }


    // Create new views (invoked by the layout manager)
    @Override
    public PastLocationViewHolder onCreateViewHolder(ViewGroup parent, int viewType) {
        // create a new view
        CardView v = (CardView)LayoutInflater.from(parent.getContext())
                .inflate(R.layout.row, parent, false);
        vh = new PastLocationViewHolder(v);
        return vh;
    }

    @Override
    public void onBindViewHolder(@NonNull PastLocationViewHolder vh, int position) {
        ParsedPastLocationReport report = pastLocationReports.get(position);
        vh.pastDeathsTextView.setText(getPastDeathsMessage(report));
        vh.pastCasesTextView.setText(getPastCasesMessage(report));
    }

    private void setPastDeathsTextView(ParsedPastLocationReport r) {
        String message = r.countyName + " New deaths: " + r.newDeathNumber + " Total Deaths: " + r.totalDeaths;
        vh.pastDeathsTextView.setText(message);
    }
    private String getPastDeathsMessage(ParsedPastLocationReport r) {
        String message = r.countyName + " New deaths: " + r.newDeathNumber + " Total Deaths: " + r.totalDeaths;
        return message;
    }

    private String getPastCasesMessage(ParsedPastLocationReport r) {
        String message = r.countyName + " New cases: " + r.newCaseNumber + "  Active Cases: " + r.activeCasesEst + " Total Cases: " + r.totalCases;
        return message;
    }

    // Return the size of your dataset (invoked by the layout manager)
    @Override
    public int getItemCount() {
        if(pastLocationReports != null) {
            return pastLocationReports.size();
        }
        else {
            return 0;
        }
    }

}