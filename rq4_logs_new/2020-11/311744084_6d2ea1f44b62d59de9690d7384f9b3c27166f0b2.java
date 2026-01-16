package com.partnersincrime.foxdarkmaster.geekeventsmobileapp.Activities;

import android.content.Context;
import android.os.Bundle;
import android.support.design.widget.TabLayout;
import android.support.v4.app.Fragment;
import android.support.v4.app.FragmentManager;
import android.support.v4.app.FragmentStatePagerAdapter;
import android.support.v4.app.NavUtils;
import android.support.v4.view.ViewPager;
import android.support.v7.widget.Toolbar;
import android.util.Log;
import android.view.Menu;
import android.view.MenuItem;

import com.partnersincrime.foxdarkmaster.geekeventsmobileapp.Adapters.MainPagerAdapter;
import com.partnersincrime.foxdarkmaster.geekeventsmobileapp.Fragments.ActivityCurrentFragment;
import com.partnersincrime.foxdarkmaster.geekeventsmobileapp.Fragments.ActivityDoneFragment;
import com.partnersincrime.foxdarkmaster.geekeventsmobileapp.Fragments.ActivityNextFragment;
import com.partnersincrime.foxdarkmaster.geekeventsmobileapp.Fragments.MainFragment;
import com.partnersincrime.foxdarkmaster.geekeventsmobileapp.Managers.ActivitiesManager;
import com.partnersincrime.foxdarkmaster.geekeventsmobileapp.R;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by foxdarkmaster on 29-07-2016.
 */
public class ActivityContainerActivities extends BaseActivity {
    private Toolbar toolbar;
    private TabLayout tabLayout;
    private ViewPager viewPager;
    private MainPagerAdapter pagerAdapter;

    @Override
    protected int getLayoutResource() {
        return R.layout.activities_swipe_with_tabs;
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        getMenuInflater().inflate(R.menu.menu_activity_container, menu);

        return true;
    }

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activities_swipe_with_tabs);

        setViews();
        setActionBar();
    }

    private void setActionBar() {
        toolbar = (Toolbar) findViewById(R.id.toolbar);

        setSupportActionBar(toolbar);
        getSupportActionBar().setDisplayHomeAsUpEnabled(true);
        getSupportActionBar().setTitle("");
    }

    private void setViews() {
        viewPager = (ViewPager) findViewById(R.id.viewpager);

        setupViewPager();

        tabLayout = (TabLayout) findViewById(R.id.tabs);
        tabLayout.setupWithViewPager(viewPager);
    }

    @Override
    public boolean onOptionsItemSelected(MenuItem item) {
        switch (item.getItemId()) {
            case android.R.id.home:
                NavUtils.navigateUpFromSameTask(this);
                return true;
            case R.id.action_day_1:
                ActivitiesManager.getInstance().setActivitiesSelectedDay(1);
                pagerAdapter.setNewData();

                return true;
            case R.id.action_day_2:
                ActivitiesManager.getInstance().setActivitiesSelectedDay(2);
                pagerAdapter.setNewData();

                return true;
            default:
                return super.onOptionsItemSelected(item);
        }
    }

    @Override
    public void onResume() {
        super.onResume();

        pagerAdapter.setNewData();
    }

    private void setupViewPager() {

        ArrayList<MainFragment> objs = new ArrayList<>();

        objs.add(MainFragment.newInstance(
                MainFragment.TYPE_NEXT, getResources().getString(R.string.fragment_title_next)));
        objs.add(MainFragment.newInstance(
                MainFragment.TYPE_CURRENT, getResources().getString(R.string.fragment_title_current)));
        objs.add(MainFragment.newInstance(
                MainFragment.TYPE_DONE, getResources().getString(R.string.fragment_title_done)));

        pagerAdapter = new MainPagerAdapter(getSupportFragmentManager(), this, objs);

        viewPager.setAdapter(pagerAdapter);
    }
}