package io.happylrd.daphne.adapter;

import android.content.Context;
import android.support.v4.app.Fragment;
import android.support.v4.app.FragmentManager;
import android.support.v4.app.FragmentStatePagerAdapter;

import io.happylrd.daphne.R;
import io.happylrd.daphne.fragment.PageFragment;
import io.happylrd.youbo.common.util.TestUtil;

/**
 * <h1>modify class name here</h1>
 * io.happylrd.daphne.adapter
 * <p>
 * add description here
 *
 * @author Crepusculo
 * @version 1.0
 * @since 2017/7/5
 */

public class MainActivityPagerAdapter extends FragmentStatePagerAdapter {
    private int[] mResIdList;
    private Context mContext;

    public MainActivityPagerAdapter(FragmentManager fm, int[] resList, Context ctx) {
        super(fm);
        this.mResIdList = resList;
        this.mContext = ctx;
    }

    @Override
    public Fragment getItem(int position) {
        TestUtil.PrintLog("p1:" + position);
        int type = mResIdList[position];
        TestUtil.PrintLog("p2:" + type);
        return PageFragment.newInstance(type);
    }

    @Override
    public int getCount() {
        return mResIdList.length;
    }

    @Override
    public CharSequence getPageTitle(int position) {
        /// TODO: using super method to return '', replace string with icon after
//        return super.getPageTitle(position);
        return getLayoutName(mResIdList[position]);
    }

    private CharSequence getLayoutName(int i) {
        switch (i) {
            case R.layout.fragment_star:
                return mContext.getResources().getString(R.string.fragment_star);
            case R.layout.fragment_suggest:
                return mContext.getResources().getString(R.string.fragment_suggest);
            default:
                return mContext.getResources().getString(R.string.fragment_suggest);
        }
    }
}