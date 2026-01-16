package com.tline.android.features.timeline.activity.view.impl;

import android.content.Intent;
import android.os.Bundle;
import android.support.annotation.NonNull;
import android.support.design.widget.BottomNavigationView;
import android.support.v4.app.Fragment;
import android.support.v4.app.FragmentTransaction;
import android.view.Menu;
import android.view.MenuItem;

import com.tline.android.R;
import com.tline.android.app.injection.AppComponent;
import com.tline.android.app.presenter.loader.PresenterFactory;
import com.tline.android.app.view.impl.BaseActivity;
import com.tline.android.features.login.view.impl.LoginActivity;
import com.tline.android.features.timeline.activity.injection.DaggerTimelineViewComponent;
import com.tline.android.features.timeline.activity.injection.TimelineViewModule;
import com.tline.android.features.timeline.activity.presenter.TimelinePresenter;
import com.tline.android.features.timeline.fragment.view.impl.TweetsFragment;
import com.tline.android.features.timeline.activity.view.TimelineView;
import com.tline.android.utils.LocaleHelper;
import com.twitter.sdk.android.core.TwitterCore;

import javax.inject.Inject;

import butterknife.BindView;
import timber.log.Timber;

public final class TimelineActivity extends BaseActivity<TimelinePresenter, TimelineView> implements TimelineView {

    private static final String SELECTED_NAV_ITEM = "SELECTED_NAV_ITEM";
    @Inject
    PresenterFactory<TimelinePresenter> mPresenterFactory;

    @BindView(R.id.bottom_navigation)
    BottomNavigationView mBottomNavigationView;

    private Fragment mContentFragment;
    private int mSelectedNavItem;

    @Override
    protected void setupComponent(@NonNull AppComponent parentComponent) {
        DaggerTimelineViewComponent.builder()
                .appComponent(parentComponent)
                .timelineViewModule(new TimelineViewModule())
                .build()
                .inject(this);
    }

    @NonNull
    @Override
    protected PresenterFactory<TimelinePresenter> getPresenterFactory() {
        return mPresenterFactory;
    }

    @Override
    protected int getContentView() {
        return R.layout.activity_timeline;
    }

    @Override
    protected void onViewReady(Bundle savedInstanceState, Intent intent) {
        super.onViewReady(savedInstanceState, intent);

        prepareBottomNavigation();

        mContentFragment = new TweetsFragment();

//        MenuItem selectedItem;
//        if (savedInstanceState != null) {
//            mSelectedNavItem = savedInstanceState.getInt(SELECTED_NAV_ITEM, 0);
//            selectedItem = mBottomNavigationView.getMenu().findItem(mSelectedNavItem);
//        } else {
//            selectedItem = mBottomNavigationView.getMenu().getItem(0);
//        }
//        selectFragment(selectedItem);
    }


    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        getMenuInflater().inflate(R.menu.menu, menu);
        return true;
    }

    @Override
    public boolean onOptionsItemSelected(MenuItem item) {
        int id = item.getItemId();

        switch (id) {
            case R.id.action_logout:
                Timber.e("R.id.action_logout");
                logoutTwitter();
                startLoginActivity();
                return true;
            case R.id.action_language:
                LocaleHelper.switchLocale(this);
                return true;
        }


        return super.onOptionsItemSelected(item);
    }

    @Override
    protected void onSaveInstanceState(Bundle outState) {
        outState.putInt(SELECTED_NAV_ITEM, mSelectedNavItem);
        super.onSaveInstanceState(outState);
    }

    @Override
    protected void onRestoreInstanceState(Bundle savedInstanceState) {
        super.onRestoreInstanceState(savedInstanceState);

        mSelectedNavItem = savedInstanceState.getInt(SELECTED_NAV_ITEM);
        selectFragment(mBottomNavigationView.getMenu().findItem(mSelectedNavItem));

    }

    private void prepareBottomNavigation() {
        mBottomNavigationView.setOnNavigationItemSelectedListener(
                new BottomNavigationView.OnNavigationItemSelectedListener() {
                    @Override
                    public boolean onNavigationItemSelected(@NonNull MenuItem item) {
                        selectFragment(item);
                        return true;
                    }
                }
        );
    }

    private void startLoginActivity() {
        Intent intent = new Intent(this, LoginActivity.class);
        startActivity(intent);
        finish();
    }

    private void selectFragment(MenuItem item) {

        switch (item.getItemId()) {
            case R.id.action_timeline:
                Timber.e("R.id.action_timeline");
                mContentFragment = TweetsFragment.newInstance(TwitterCore.getInstance().getSessionManager().getActiveSession().getUserName());
                break;
            case R.id.action_olx_egypt:
                Timber.e("R.id.action_olx_egypt");
                mContentFragment = TweetsFragment.newInstance("@OLXEgypt");
                break;
            case R.id.action_android_dev:
                Timber.e("R.id.action_android_dev");
                mContentFragment = TweetsFragment.newInstance("@AndroidDev");
                break;
        }

        // update selected item
        mSelectedNavItem = item.getItemId();

        FragmentTransaction ft = getSupportFragmentManager().beginTransaction();
        ft.add(R.id.container, mContentFragment, mContentFragment.getTag());
        ft.commit();

    }
}