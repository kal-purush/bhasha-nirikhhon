package adapters;

import android.content.Context;
import android.support.v4.app.Fragment;
import android.support.v4.app.FragmentManager;
import android.support.v4.app.FragmentPagerAdapter;

import com.concon.talkabout.talkabout.CustomRulesFragment;

/**
 * Created by Pablitoh on 02/06/2015.
 */
public class RulesPageAdapter extends FragmentPagerAdapter {

    final int PAGE_COUNT = 3;
    private String tabTitles[] = new String[] { "My Rules", "Popular", "Recently Added" };
    private Context context;

    public RulesPageAdapter(FragmentManager fm, Context context)
    {
        super(fm);
        this.context = context;
    }

    @Override
    public int getCount()
    {
        return PAGE_COUNT;
    }

    @Override
    public Fragment getItem(int position) {
        switch (position) {
            case 0: return new CustomRulesFragment();
            case 1:
                return new CustomRulesFragment();
            case 2:
                return new CustomRulesFragment();
        }
        return null;
    }


    @Override
    public CharSequence getPageTitle(int position) {
        return tabTitles[position];
    }
}