package com.numan1617.tfltubedepartures.feature.stopdetail;

import android.support.annotation.NonNull;
import com.numan1617.tfltubedepartures.base.BasePresenter;
import com.numan1617.tfltubedepartures.base.PresenterView;
import com.numan1617.tfltubedepartures.network.model.StopPoint;
import rx.Scheduler;

class StopDetailPresenter extends BasePresenter<StopDetailPresenter.View> {
  private final Scheduler uiScheduler;
  private final Scheduler ioScheduler;
  private View view;

  public StopDetailPresenter(@NonNull final Scheduler uiScheduler,
      @NonNull final Scheduler ioScheduler) {
    this.uiScheduler = uiScheduler;
    this.ioScheduler = ioScheduler;
  }

  @Override public void onViewAttached(@NonNull final View view) {
    super.onViewAttached(view);
    this.view = view;
  }

  public void setStopPoint(final StopPoint stopPoint) {
    view.setStopName(stopPoint.commonName());
  }

  interface View extends PresenterView {
    void setStopName(@NonNull String stopName);
  }
}