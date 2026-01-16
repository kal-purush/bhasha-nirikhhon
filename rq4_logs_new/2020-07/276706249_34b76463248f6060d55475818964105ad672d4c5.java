package com.dtavana.foodswipe.utils;

import android.location.Location;

import androidx.fragment.app.Fragment;
import androidx.fragment.app.FragmentManager;

import com.dtavana.foodswipe.R;
import com.dtavana.foodswipe.fragments.CycleFragment;
import com.dtavana.foodswipe.fragments.FilterFragment;
import com.dtavana.foodswipe.models.RestaurantFilter;

import java.util.HashMap;
import java.util.List;

public class FilterFirst {

    public static boolean filtered = false;

    public static void run(FragmentManager manager, HashMap<FilterFragment.ALL_FILTERS, List<RestaurantFilter>> filterCache, Location location) {
        String TAG;
        Fragment fragment;

        if(filtered) {
            TAG = CycleFragment.TAG;
            fragment = manager.findFragmentByTag(TAG);
            if(fragment == null) {
                fragment = CycleFragment.newInstance(filterCache, location);
            }
        }
        else {
            TAG = FilterFragment.TAG;
            fragment = manager.findFragmentByTag(TAG);
            if(fragment == null) {
                fragment = new FilterFragment();
            }
        }
        manager.beginTransaction().replace(R.id.flContainer, fragment, TAG).addToBackStack(TAG).commit();
    }

}