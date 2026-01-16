package com.example.trekkin.ui.explore;

import android.content.Context;

import androidx.fragment.app.Fragment;
import androidx.fragment.app.FragmentManager;
import androidx.fragment.app.FragmentPagerAdapter;

import com.example.trekkin.ui.community.fragments.FriendsCommunityFragment;
import com.example.trekkin.ui.community.fragments.GroupsCommunityFragment;
import com.example.trekkin.ui.explore.fragments.FavouriteExploreFragment;
import com.example.trekkin.ui.explore.fragments.SuggestedExploreFragment;

import org.jetbrains.annotations.NotNull;

public class SectionsPagerAdapter extends FragmentPagerAdapter {
    public SectionsPagerAdapter(Context context, FragmentManager fm) {
        super(fm);
    }

    @NotNull
    @Override
    public Fragment getItem(int position) {
        if(position == 0){
            return FavouriteExploreFragment.newInstance();
        }
        else return SuggestedExploreFragment.newInstance();
    }

    @Override
    public int getCount() {
        // Show 2 total pages.
        return 2;
    }
}