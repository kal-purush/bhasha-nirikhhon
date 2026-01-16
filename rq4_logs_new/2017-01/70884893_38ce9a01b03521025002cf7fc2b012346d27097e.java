package com.wl.demo.mvpsample.base;

import android.support.v4.app.Fragment;

import com.wl.demo.mvpsample.net.SubscriptionManager;

/**
 * Created by wangliang on 17-1-19.
 */
public abstract class BaseFragment extends Fragment {

    //用于唯一标示该Activity
    public abstract String getTagName();

    @Override
    public void onDestroy() {
        super.onDestroy();
        SubscriptionManager.getInstance().cancelPendingRequests(getTagName());
    }

}