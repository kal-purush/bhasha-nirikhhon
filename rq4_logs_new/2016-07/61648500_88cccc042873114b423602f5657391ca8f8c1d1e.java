package com.softdesign.devintensive.ui.behaviors;

import android.content.Context;
import android.support.design.widget.AppBarLayout;
import android.support.design.widget.CoordinatorLayout;
import android.util.AttributeSet;
import android.view.View;
import android.widget.CursorAdapter;

import com.softdesign.devintensive.R;
import com.softdesign.devintensive.utils.UiHelper;

/**
 * Created by alena on 12.07.16.
 */
public class CustomNastedScrollBehavior extends AppBarLayout.ScrollingViewBehavior {

    private final int mMaxAppBarHeight;
    private final int mMinAppBarHeight;
    private final int mMaxUserInfoHeight;

    public CustomNastedScrollBehavior(Context context, AttributeSet attrs){
        super(context, attrs);

        mMinAppBarHeight = UiHelper.getStatusBarHeight() + UiHelper.getActionBarHeight();
        mMaxAppBarHeight = R.dimen.profile_image_size_256;
        mMaxUserInfoHeight = R.dimen.size_112;
    }

    @Override
    public boolean onDependentViewChanged(CoordinatorLayout parent, View child, View dependency) {

        float friction = UiHelper.currentFriction(mMinAppBarHeight, mMaxAppBarHeight, dependency.getBottom());
        int transY = UiHelper.lerp(mMaxUserInfoHeight/2, mMaxUserInfoHeight, friction);
        CoordinatorLayout.LayoutParams lp = (CoordinatorLayout.LayoutParams) child.getLayoutParams();
        lp.topMargin = transY;
        child.setLayoutParams(lp);

        return super.onDependentViewChanged(parent, child, dependency);
    }

    @Override
    public boolean layoutDependsOn(CoordinatorLayout parent, View child, View dependency) {
        return dependency instanceof AppBarLayout;
    }
}