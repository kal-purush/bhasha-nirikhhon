package com.example.myapplicationbehavior;

import android.content.Context;
import android.util.AttributeSet;
import android.util.Log;
import android.view.View;
import android.widget.TextView;

import androidx.annotation.NonNull;
import androidx.coordinatorlayout.widget.CoordinatorLayout;
import androidx.core.view.ViewCompat;
import androidx.recyclerview.widget.LinearLayoutManager;
import androidx.recyclerview.widget.RecyclerView;

public class HeaderBehavior extends CoordinatorLayout.Behavior<TextView> {
    private String TAG = HeaderBehavior.class.getSimpleName();

    public HeaderBehavior() {
        super();
    }

    public HeaderBehavior(Context context, AttributeSet attrs) {
        super(context, attrs);
    }

    @Override
    public boolean onStartNestedScroll(@NonNull CoordinatorLayout coordinatorLayout, @NonNull TextView child, @NonNull View directTargetChild, @NonNull View target, int nestedScrollAxes, int type) {
        return (nestedScrollAxes & ViewCompat.SCROLL_AXIS_VERTICAL) != -1;//判断是否为垂直滚动;
    }

    @Override
    public void onNestedPreScroll(@NonNull CoordinatorLayout coordinatorLayout, @NonNull TextView child, @NonNull View target, int dx, int dy, @NonNull int[] consumed, int type) {
        super.onNestedPreScroll(coordinatorLayout, child, target, dx, dy, consumed, type);
        if (target instanceof RecyclerView) {
            RecyclerView recyclerView = (RecyclerView) target;
            LinearLayoutManager linearLayoutManager = (LinearLayoutManager) recyclerView.getLayoutManager();
            if (dy > 0) {//手指向上滑动
                //判断recyclerview里面的内容是否能够继续滑动
                if (recyclerView.canScrollVertically(1)) {
                    if (Math.abs(child.getTranslationY()) != child.getHeight()) {
                        consumed[1] = dy;
                    }
                    if (Math.abs(child.getTranslationY()) >= child.getHeight()) {
                        child.setTranslationY(-child.getHeight());
                    } else {
                        float disy = child.getTranslationY() - dy;
                        child.setTranslationY(disy);
                    }
                }
            } else {//手指向下滑动
                Log.i(TAG,"getChildAt(0).getTop()："+ linearLayoutManager.getChildAt(0).getTop());
                if (recyclerView.canScrollVertically(-1)){//)表示是否能向下滚动

                }else {
                    if (child.getTranslationY() >= 0) {
                        child.setTranslationY(0);
                    } else {
                        float disy = child.getTranslationY() - dy;
                        child.setTranslationY(disy);
                    }
                }
            }
        }
    }
}