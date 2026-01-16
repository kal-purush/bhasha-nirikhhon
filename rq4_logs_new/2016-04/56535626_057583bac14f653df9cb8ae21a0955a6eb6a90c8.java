package com.numan1617.tfltubedepartures.feature.main;

import android.support.v7.widget.RecyclerView;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import com.numan1617.tfltubedepartures.R;
import com.numan1617.tfltubedepartures.network.model.StopPoint;
import java.util.ArrayList;
import java.util.List;
import rx.Observable;
import rx.subjects.PublishSubject;

class StopListAdapter extends RecyclerView.Adapter<StopPointViewHolder> {

  public final PublishSubject<StopPoint> stopPointSelected = PublishSubject.create();
  private List<StopPoint> stopPoints = new ArrayList<>();

  @Override
  public StopPointViewHolder onCreateViewHolder(final ViewGroup parent, final int viewType) {
    final View inflatedView =
        LayoutInflater.from(parent.getContext()).inflate(R.layout.list_stop_point, parent, false);
    final StopPointViewHolder searchViewHolder = new StopPointViewHolder(inflatedView);
    inflatedView.setOnClickListener(view -> {
      stopPointSelected.onNext(searchViewHolder.getStopPoint());
    });
    return searchViewHolder;
  }

  @Override public void onBindViewHolder(final StopPointViewHolder holder, final int position) {
    holder.bindData(stopPoints.get(position));
  }

  @Override public int getItemCount() {
    return stopPoints.size();
  }

  public Observable<StopPoint> stopPointSelected() {
    return stopPointSelected;
  }

  public void setData(final List<StopPoint> stopPoints) {
    this.stopPoints = stopPoints;
    notifyDataSetChanged();
  }
}