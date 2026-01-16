package com.wastedtimecounter.fragments;

import android.content.Context;
import android.support.v4.app.Fragment;
import android.support.v4.app.FragmentManager;
import android.support.v4.app.FragmentPagerAdapter;

/**
 * Created by student on 21.01.2016.
 */
public class FragmentPageAdapter extends FragmentPagerAdapter {
    private int PAGE_COUNT = 3;
    private String[] tabTitles = new String[] { "Tab1", "Tab2", "Tab3" };
    private Context context;

    public FragmentPageAdapter(FragmentManager fm, Context context, int pages, String[] tabs) {
        super(fm);
        this.context = context;
        PAGE_COUNT = pages;
        tabTitles = tabs;
    }

    @Override
    public int getCount() {
        return PAGE_COUNT;
    }

    @Override
    public Fragment getItem(int position) {
        return PageFragment.newInstance(position);
    }

    @Override
    public CharSequence getPageTitle(int position) {
        // Generate title based on item position
        return tabTitles[position];
    }
}