package com.example.trekkin.ui.community;

import android.content.Context;

import androidx.annotation.Nullable;
import androidx.fragment.app.Fragment;
import androidx.fragment.app.FragmentManager;
import androidx.fragment.app.FragmentPagerAdapter;
import androidx.viewpager.widget.PagerAdapter;

import com.example.trekkin.ui.community.fragments.FriendsCommunityFragment;
import com.example.trekkin.ui.community.fragments.GroupsCommunityFragment;

import org.jetbrains.annotations.NotNull;

public class SectionsPagerAdapter extends FragmentPagerAdapter {
    public SectionsPagerAdapter(Context context, FragmentManager fm) {
        super(fm);
    }

    @NotNull
    @Override
    public Fragment getItem(int position) {
        if(position == 0){
            return FriendsCommunityFragment.newInstance();
        }
        else return GroupsCommunityFragment.newInstance();
    }

    @Override
    public int getCount() {
        // Show 2 total pages.
        return 2;
    }
}