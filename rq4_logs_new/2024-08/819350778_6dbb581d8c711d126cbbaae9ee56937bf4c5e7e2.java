package com.app.Regional_News;

import android.os.Bundle;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ImageView;
import android.widget.TextView;

import androidx.annotation.NonNull;
import androidx.fragment.app.Fragment;
import androidx.fragment.app.FragmentPagerAdapter;
import androidx.viewpager.widget.ViewPager;

import com.app.Regional_News.EditorialFragment;
import com.app.Regional_News.EntertainmentFragment;
import com.app.Regional_News.R;
import com.app.Regional_News.OlympicsFragment;
import com.app.Regional_News.SportsFragment;
import com.app.Regional_News.StockMarketFragment;
import com.app.Regional_News.adapter.VPAdapter;
import com.app.Regional_News.latest_news;
import com.google.android.material.tabs.TabLayout;


public class HomeFragment extends Fragment {

    private TabLayout tabLayout;
    private ViewPager viewPager;

    public View onCreateView(@NonNull LayoutInflater inflater,
                             ViewGroup container, Bundle savedInstanceState) {
        View rootView = inflater.inflate(R.layout.fragment_home, container, false);

        tabLayout = rootView.findViewById(R.id.tablayout);
        viewPager = rootView.findViewById(R.id.viewpager);

        VPAdapter vpAdapter = new VPAdapter(getChildFragmentManager(), FragmentPagerAdapter.BEHAVIOR_RESUME_ONLY_CURRENT_FRAGMENT);
        vpAdapter.addFragment(new latest_news(),getString(R.string.home_tab_latest_news));
        vpAdapter.addFragment(new OlympicsFragment(),getString(R.string.home_olympic));
        vpAdapter.addFragment(new StockMarketFragment(),getString(R.string.home_stock_news));
        vpAdapter.addFragment(new SportsFragment(),getString(R.string.home_sport_news));
        vpAdapter.addFragment(new EntertainmentFragment(),getString(R.string.home_entertainment_news));
        vpAdapter.addFragment(new EditorialFragment(),getString(R.string.home_editoral_news));
        viewPager.setAdapter(vpAdapter);
        tabLayout.setupWithViewPager(viewPager);

        setUpTabIcons();


        return rootView;
    }

    private void setUpTabIcons() {

        int[] tabIcons = {
                R.drawable.ic_trending,
                R.drawable.ic_olympic,
                R.drawable.ic_news_headline,
                R.drawable.ic_temple,
                R.drawable.ic_entertainment,
                R.drawable.ic_tentri_lekh        };


        for (int i = 0; i < tabLayout.getTabCount(); i++) {
            TabLayout.Tab tab = tabLayout.getTabAt(i);
            if (tab != null) {
                View customView = LayoutInflater.from(getContext()).inflate(R.layout.tab_item, null);
                ImageView tabIcon = customView.findViewById(R.id.tab_icon);
                TextView tabText = customView.findViewById(R.id.tab_text);
                tabIcon.setImageResource(tabIcons[i]);
                tabText.setText(tab.getText());
                tab.setCustomView(customView);
            }
        }
    }


    @Override
    public void onDestroyView() {
        super.onDestroyView();
    }
}