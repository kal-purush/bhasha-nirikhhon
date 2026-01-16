package com.taxi.clickcar;

import android.content.SharedPreferences;
import android.content.res.ColorStateList;
import android.os.Bundle;
import android.support.annotation.Nullable;
import android.support.design.widget.TabLayout;
import android.support.v4.app.Fragment;
import android.support.v4.app.FragmentManager;
import android.support.v4.app.FragmentPagerAdapter;
import android.support.v4.view.ViewPager;
import android.support.v7.app.AppCompatActivity;
import android.view.View;
import android.widget.Toast;

import com.taxi.clickcar.Fragments.EnterFragment;
import com.taxi.clickcar.Fragments.MyFragmentListener;
import com.taxi.clickcar.Fragments.RootFragment;
import com.taxi.clickcar.Fragments.RootFragmentReg;

/**
 * Created by Назар on 29.09.2016.
 */
public class FirstRegister extends AppCompatActivity {
    public SharedPreferences mSettings;
    public View.OnClickListener mOnClickListener;
    public static String name="";
    public ViewPager pager=null;
    public static String phone="";
    public MyFragmentListener myFragmentListener;

    @Override
    protected void onCreate(@Nullable Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.first_registration);
        final TabLayout tabs = (TabLayout) findViewById(R.id.tabs);
        tabs.setBackgroundResource(R.color.mainblue);
        tabs.setTabTextColors(ColorStateList.valueOf(getResources().getColor(R.color.white)));
        pager = (ViewPager) findViewById(R.id.pager);
        TabsPagerAdapter adapter = new TabsPagerAdapter(getSupportFragmentManager());
        pager.setAdapter(adapter);
        tabs.setupWithViewPager(pager);

        StaticMethods.CheckConnection(this,mOnClickListener,getString(R.string.error_connection));

    }

    public class TabsPagerAdapter extends FragmentPagerAdapter {

        public TabsPagerAdapter(FragmentManager fm) {
            super(fm);
        }
        @Override
        public Fragment getItem(int position) {
               return new RootFragmentReg();
        }

        @Override
        public int getCount() {
            return 1;
        }

        @Override
        public CharSequence getPageTitle(int position) {
             return getString(R.string.label_register);
        }
    }

    public void setmOnClickListener(View.OnClickListener mOnClickListener) {
        this.mOnClickListener = mOnClickListener;
    }
}