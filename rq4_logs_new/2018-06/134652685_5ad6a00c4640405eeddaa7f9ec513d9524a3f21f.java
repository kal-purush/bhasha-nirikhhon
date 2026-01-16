package ca.huynhat.itsasteal;

import android.content.Context;
import android.support.annotation.NonNull;
import android.support.v7.widget.RecyclerView;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.BaseAdapter;

public class HomeRecyclerAdapter extends RecyclerView.Adapter<HomeRecyclerAdapter.AppViewHolder> {

    //Empty Constructor
    HomeRecyclerAdapter(){

    }


    @NonNull
    @Override
    public AppViewHolder onCreateViewHolder(@NonNull ViewGroup parent, int viewType) {
        View itemView = LayoutInflater.from(parent.getContext())
                .inflate(R.layout.deal_row_item, parent, false);

        return new AppViewHolder(itemView);
    }

    @Override
    public void onBindViewHolder(@NonNull AppViewHolder holder, int position) {

    }

    @Override
    public int getItemCount() {
        return 3;
    }


    public class AppViewHolder extends RecyclerView.ViewHolder {
        public AppViewHolder(View itemView) {
            super(itemView);
        }
    }
}