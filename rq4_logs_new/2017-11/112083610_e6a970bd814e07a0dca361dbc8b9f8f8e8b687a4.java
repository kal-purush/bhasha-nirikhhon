package com.tline.android.features.timeline;

import com.tline.android.features.timeline.activity.interactor.TimelineInteractor;
import com.tline.android.features.timeline.activity.presenter.TimelinePresenter;
import com.tline.android.features.timeline.activity.presenter.impl.TimelinePresenterImpl;
import com.tline.android.features.timeline.activity.view.TimelineView;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import static org.mockito.Mockito.verify;

/**
 * Created by Naeem(naeemark@gmail.com)
 * On 30/11/2017.
 * For TLine
 */
public class TimelinePresenterTest {

    @Rule
    public MockitoRule mockitoRule = MockitoJUnit.rule();

    @Mock
    private TimelineInteractor mInteractor;

    @Mock
    private TimelineView mView;

    private TimelinePresenter mPresenter;

    @Before
    public void setUp() throws Exception {
        mPresenter = new TimelinePresenterImpl(mInteractor);
        mPresenter.onViewAttached(mView);
    }

    @Test
    public void shouldLogout() throws Exception{

        mPresenter.logout();

        verify(mView).logoutTwitter();
    }

    @Test
    public void shouldLaunchLoginActivity() throws Exception{

        mPresenter.logout();

        verify(mView).launchLoginActivity();
    }

    @Test
    public void shouldShowFragment() throws Exception{

        mPresenter.onStart(true);

        verify(mView).showInitialFragment();
    }




}