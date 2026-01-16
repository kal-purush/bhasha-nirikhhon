package com.hankou.utils.behavior;

import android.content.Context;
import android.os.Handler;
import android.support.design.widget.CoordinatorLayout;
import android.support.v4.view.ViewCompat;
import android.util.AttributeSet;
import android.util.Log;
import android.view.View;
import android.widget.ImageView;
import android.widget.Scroller;

import com.hankou.utils.views.AutoRecyclerView;

/**
 * Created by bykj003 on 2016/12/15.
 */

public class RecyclerViewBehavior extends CoordinatorLayout.Behavior<View> {

    public static final String TAG = "ImageBehavior";

    private Scroller mScroller;

    private Handler mHandler;

    private View mDependencyView;

    private int mDependencyHeight;

    private STATE mState = STATE.NONE;

    public enum STATE {
        TOP, BOTTOM, NONE
    }

    public RecyclerViewBehavior(Context context, AttributeSet attrs) {
        super(context, attrs);
        mScroller = new Scroller(context);
        mHandler = new Handler();
    }

    @Override
    public boolean layoutDependsOn(CoordinatorLayout parent, View child, View dependency) {
        mDependencyView = dependency;
        mDependencyHeight = dependency.getMeasuredHeight();
        return dependency instanceof ImageView;
    }

    @Override
    public boolean onDependentViewChanged(CoordinatorLayout parent, View child, View dependency) {
        child.setTranslationY(mDependencyHeight + dependency.getTranslationY());
        return super.onDependentViewChanged(parent, child, dependency);
    }

    @Override
    public void onNestedScrollAccepted(CoordinatorLayout coordinatorLayout, View child, View directTargetChild, View target, int nestedScrollAxes) {
        super.onNestedScrollAccepted(coordinatorLayout, child, directTargetChild, target, nestedScrollAxes);
        mScroller.abortAnimation();
    }

    @Override
    public boolean onStartNestedScroll(CoordinatorLayout coordinatorLayout, View child, View directTargetChild, View target, int nestedScrollAxes) {
        return nestedScrollAxes == ViewCompat.SCROLL_AXIS_VERTICAL;
    }

    @Override
    public void onNestedPreScroll(CoordinatorLayout coordinatorLayout, View child, View target,
                                  int dx, int dy, int[] consumed) {
        super.onNestedPreScroll(coordinatorLayout, child, target, dx, dy, consumed);
        if (dy < 0) {
            return;
        }
        float newTranslationY = (mDependencyView.getTranslationY() - dy);
        if (newTranslationY >= -mDependencyHeight) {
            mState = STATE.TOP;
            mDependencyView.setTranslationY(newTranslationY);
            consumed[1] = dy;
        }
    }

    @Override
    public void onNestedScroll(CoordinatorLayout coordinatorLayout, View child, View target,
                               int dxConsumed, int dyConsumed, int dxUnconsumed, int dyUnconsumed) {
        super.onNestedScroll(coordinatorLayout, child, target, dxConsumed, dyConsumed, dxUnconsumed, dyUnconsumed);
        if (dyUnconsumed > 0) {
            return;
        }
        float newTranslationY = mDependencyView.getTranslationY() - dyUnconsumed;
        if (newTranslationY <= 0) {
            mState = STATE.BOTTOM;
            mDependencyView.setTranslationY(newTranslationY);
        }
    }

    @Override
    public boolean onNestedFling(CoordinatorLayout coordinatorLayout, View child, View target, float velocityX, float velocityY, boolean consumed) {
        Log.i(TAG, "onNestedFling()" + "State:" + mState);
        scrollToTarget();
        return super.onNestedFling(coordinatorLayout, child, target, velocityX, velocityY, consumed);
    }

    @Override
    public void onStopNestedScroll(CoordinatorLayout coordinatorLayout, View child, View target) {
        super.onStopNestedScroll(coordinatorLayout, child, target);
        Log.i(TAG, "onStopNestedScroll()" + "State:" + mState);
        scrollToTarget();
    }

    private void scrollToTarget() {
        mHandler.removeCallbacks(runnable);
        int dy = 0;
        if (mState == STATE.TOP) {
            dy = (-mDependencyHeight - (int) mDependencyView.getTranslationY());
        } else if (mState == STATE.BOTTOM) {
            dy = (int) (-mDependencyView.getTranslationY());
        } else {
            return;
        }
        mScroller.startScroll(0, (int) mDependencyView.getTranslationY(), 0, dy);
        mHandler.post(runnable);
    }

    private Runnable runnable = new Runnable() {
        @Override
        public void run() {
            mState = STATE.NONE;
            if (mScroller.computeScrollOffset()) {
                mDependencyView.setTranslationY(mScroller.getCurrY());
                mHandler.post(this);
            }
        }
    };
}