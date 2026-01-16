package com.example.phuong.viectimnguoiapp.activities;

import android.os.Build;
import android.support.v4.app.FragmentTransaction;
import android.support.v4.view.GravityCompat;
import android.support.v4.widget.DrawerLayout;
import android.support.v7.app.ActionBarDrawerToggle;
import android.support.v7.widget.Toolbar;
import android.view.View;
import android.view.animation.TranslateAnimation;
import android.widget.FrameLayout;

import com.example.phuong.viectimnguoiapp.R;
import com.example.phuong.viectimnguoiapp.fragments.MainFragment_;
import com.example.phuong.viectimnguoiapp.fragments.SettingMenuFragment;
import com.example.phuong.viectimnguoiapp.fragments.SettingMenuFragment_;
import com.example.phuong.viectimnguoiapp.utils.ScreenUtil;

import org.androidannotations.annotations.EActivity;
import org.androidannotations.annotations.ViewById;

/**
 * Created by phuong on 20/02/2017.
 */
@EActivity(R.layout.activity_main)
public class MainActivity extends BaseActivity {
    public ActionBarDrawerToggle mDrawerToggle;
    @ViewById(R.id.toolBar)
    protected Toolbar mToolbar;
    @ViewById(R.id.drawerLayout)
    protected DrawerLayout mDrawerLayout;
    @ViewById(R.id.frLeftMenuContainer)
    protected FrameLayout mFrLeftMenuContainer;
    @ViewById(R.id.rlContainer)
    protected View mRlContainer;
    private SettingMenuFragment mSettingMenuFragment;
    private CharSequence mDrawerTitle;
    private CharSequence mTitle;
    private float mLastTranslate = 0.0f;

    @Override
    void inits() {
        initActionbar();
        initMenu();
        initMain();
    }

    public void initMain() {
        getSupportFragmentManager().beginTransaction().replace(R.id.frMainContainer, MainFragment_.builder().build()).commit();
    }

    private void initActionbar() {
        mToolbar.setNavigationIcon(R.drawable.ic_menu);
        setSupportActionBar(mToolbar);
        getSupportActionBar().setHomeAsUpIndicator(R.drawable.ic_menu);
        getSupportActionBar().setHomeButtonEnabled(true);
        getSupportActionBar().setDisplayHomeAsUpEnabled(true);
    }

    public void initMenu() {
        FragmentTransaction ft = getSupportFragmentManager().beginTransaction();
        mSettingMenuFragment = new SettingMenuFragment_();
        ft.add(R.id.frLeftMenuContainer, mSettingMenuFragment).commit();
        mTitle = mDrawerTitle = getTitle();
        mDrawerLayout.setScrimColor(getResources().getColor(R.color.drawerlayout_scrim));
        mDrawerToggle = new ActionBarDrawerToggle(this, mDrawerLayout, mToolbar, R.string.openDrawer, R.string.closeDrawer) {
            @Override
            public void onDrawerOpened(View drawerView) {
                super.onDrawerOpened(drawerView);

                getSupportActionBar().setTitle(mDrawerTitle);
                mSettingMenuFragment.onDrawerOpened(drawerView);
            }

            @Override
            public void onDrawerClosed(View drawerView) {
                super.onDrawerClosed(drawerView);

                mSettingMenuFragment.onDrawerClosed(drawerView);
            }

            @Override
            public void onDrawerSlide(View drawerView, float slideOffset) {
                super.onDrawerSlide(drawerView, slideOffset);
                float moveFactor = (mFrLeftMenuContainer.getWidth() * slideOffset);

                if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.HONEYCOMB) {
                    mRlContainer.setTranslationX(moveFactor);
                } else {
                    TranslateAnimation anim = new TranslateAnimation(mLastTranslate, moveFactor, 0.0f, 0.0f);
                    anim.setDuration(0);
                    anim.setFillAfter(true);
                    mRlContainer.startAnimation(anim);
                    mLastTranslate = moveFactor;
                }
            }
        };
        mDrawerLayout.setDrawerListener(mDrawerToggle);
        mDrawerToggle.syncState();
        mDrawerToggle.setDrawerIndicatorEnabled(false);
        mDrawerToggle.setHomeAsUpIndicator(R.drawable.ic_menu);
        mDrawerToggle.setToolbarNavigationClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                mDrawerLayout.openDrawer(GravityCompat.START);
            }
        });

        DrawerLayout.LayoutParams params = (DrawerLayout.LayoutParams) mFrLeftMenuContainer.getLayoutParams();
        params.width = ScreenUtil.getWidthScreen(this) - ScreenUtil.convertDPToPixels(this, 76);

    }
}