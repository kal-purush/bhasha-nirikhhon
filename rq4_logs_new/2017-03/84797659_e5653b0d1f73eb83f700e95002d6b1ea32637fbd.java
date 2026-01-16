package org.sogrey.actionbar;

import android.content.Context;
import android.support.annotation.Nullable;
import android.util.AttributeSet;

import org.sogrey.actionbar.base.BaseNavigatorBar;

/**
 * Created by Sogrey on 2017/3/12.
 */

public class CustomFadingActionBar extends BaseNavigatorBar {
    public CustomFadingActionBar(Context context) {
        super(context);
    }

    public CustomFadingActionBar(Context context, @Nullable AttributeSet attrs) {
        super(context, attrs);
    }

    public CustomFadingActionBar(Context context, @Nullable AttributeSet attrs, int defStyleAttr) {
        super(context, attrs, defStyleAttr);
    }

    public CustomFadingActionBar(Context context, AttributeSet attrs, int defStyleAttr, int defStyleRes) {
        super(context, attrs, defStyleAttr, defStyleRes);
    }

    @Override
    protected void initViews(Context context) {

    }

    @Override
    protected void initAttributes(Context context, AttributeSet attrs, int defStyleAttr) {

    }
}