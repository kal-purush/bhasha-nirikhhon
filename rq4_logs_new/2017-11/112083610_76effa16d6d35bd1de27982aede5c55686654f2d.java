package com.tline.android.features.timeline.injection;

import android.support.annotation.NonNull;

import com.tline.android.app.presenter.loader.PresenterFactory;
import com.tline.android.features.timeline.interactor.TimelineInteractor;
import com.tline.android.features.timeline.interactor.impl.TimelineInteractorImpl;
import com.tline.android.features.timeline.presenter.TimelinePresenter;
import com.tline.android.features.timeline.presenter.impl.TimelinePresenterImpl;

import dagger.Module;
import dagger.Provides;

@Module
public final class TimelineViewModule {
    @Provides
    public TimelineInteractor provideInteractor() {
        return new TimelineInteractorImpl();
    }

    @Provides
    public PresenterFactory<TimelinePresenter> providePresenterFactory(@NonNull final TimelineInteractor interactor) {
        return new PresenterFactory<TimelinePresenter>() {
            @NonNull
            @Override
            public TimelinePresenter create() {
                return new TimelinePresenterImpl(interactor);
            }
        };
    }
}