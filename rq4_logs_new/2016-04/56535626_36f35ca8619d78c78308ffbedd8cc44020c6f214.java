package com.numan1617.tfltubedepartures.feature.stopdetail;

import android.support.v7.widget.RecyclerView;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import com.numan1617.tfltubedepartures.R;
import com.numan1617.tfltubedepartures.network.model.Departure;
import java.util.ArrayList;
import java.util.List;

class DepartureAdapter extends RecyclerView.Adapter<DepartureViewHolder> {
  private List<Departure> departures = new ArrayList<>();

  @Override public DepartureViewHolder onCreateViewHolder(final ViewGroup parent, final int viewType) {
    final View inflatedView = LayoutInflater.from(parent.getContext()).inflate(R.layout.list_departure, parent, false);
    return new DepartureViewHolder(inflatedView);
  }

  @Override public void onBindViewHolder(final DepartureViewHolder holder, final int position) {
    holder.bindData(departures.get(position));
  }

  @Override public int getItemCount() {
    return departures.size();
  }

  public void setData(final List<Departure> departures) {
    this.departures = departures;
    notifyDataSetChanged();
  }
}