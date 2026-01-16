package com.tline.android.features.timeline;

import com.tline.android.features.timeline.activity.interactor.TimelineInteractor;
import com.tline.android.features.timeline.activity.presenter.TimelinePresenter;
import com.tline.android.features.timeline.activity.presenter.impl.TimelinePresenterImpl;
import com.tline.android.features.timeline.activity.view.TimelineView;
import com.tline.android.features.timeline.fragment.interactor.TweetsInteractor;
import com.tline.android.features.timeline.fragment.presenter.TweetsPresenter;
import com.tline.android.features.timeline.fragment.presenter.impl.TweetsPresenterImpl;
import com.tline.android.features.timeline.fragment.view.TweetsView;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import static org.junit.Assert.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Created by Naeem(naeemark@gmail.com)
 * On 30/11/2017.
 * For TLine
 */
public class TweetsPresenterTest {


    @Rule
    public MockitoRule mockitoRule = MockitoJUnit.rule();

    @Mock
    private TweetsInteractor mInteractor;

    @Mock
    private TweetsView mView;

    private TweetsPresenter mPresenter;

    @Before
    public void setUp() throws Exception {
        mPresenter = new TweetsPresenterImpl(mInteractor);
        mPresenter.onViewAttached(mView);
    }

    @Test
    public void shouldShowError() throws Exception {

        when(mInteractor.isNetworkConnected()).thenReturn(false);
        when(mInteractor.getErrorString()).thenReturn("");

        mPresenter.onStart(true);

        verify(mView).showErrorMessage("");
    }

    @Test
    public void shouldGetHandle() throws Exception {

        when(mInteractor.isNetworkConnected()).thenReturn(true);

        mPresenter.onStart(true);

        verify(mView).getTwitterHandle();
    }

    @Test
    public void shouldShowLoading() throws Exception {

        mPresenter.onStart();

        verify(mView).showLoading();
    }

    @Test
    public void shouldHideLoading() throws Exception {

        mPresenter.onComplete();

        verify(mView).hideLoading();
    }

    @Test
    public void shouldShowErrorMessage() throws Exception {

        mPresenter.onFailure("ERROR");

        verify(mView).showErrorMessage("ERROR");
    }

}