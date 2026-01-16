package ru.gdgkazan.popularmoviesclean.screen.details;

import ru.arturvasilov.rxloader.LifecycleHandler;
import ru.gdgkazan.popularmoviesclean.R;
import ru.gdgkazan.popularmoviesclean.domain.model.DetailHolder;
import ru.gdgkazan.popularmoviesclean.domain.usecase.DetailsUseCase;


public class DetailsPresenter {

    private DetailsView mDetailsView;
    private DetailsUseCase mDetalsUseCase;
    private final LifecycleHandler mLifecycleHandler;

    public DetailsPresenter(DetailsView mDetailsView, DetailsUseCase mDetalsUseCase, LifecycleHandler mLifecycleHandler) {
        this.mDetailsView = mDetailsView;
        this.mDetalsUseCase = mDetalsUseCase;
        this.mLifecycleHandler = mLifecycleHandler;
    }

    public void init(int movieID) {
        mDetalsUseCase.getDetails(movieID)
                .doOnSubscribe(mDetailsView::showLoadingIndicator)
                .doAfterTerminate(mDetailsView::hideLoadingIndicator)
                .compose(mLifecycleHandler.load(R.id.movies_request_id))
                .subscribe(this::showDetails, throwable -> mDetailsView.showError());
    }

    private void showDetails(DetailHolder holder) {
        mDetailsView.showReviews(holder.getReviews());
        mDetailsView.showTrailers(holder.getVideos());
    }
}