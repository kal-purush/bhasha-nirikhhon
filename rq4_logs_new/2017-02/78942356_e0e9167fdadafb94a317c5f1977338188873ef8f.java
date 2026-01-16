package com.psedoykin.birthday.ui.Decorator;

import android.graphics.Rect;
import android.support.v7.widget.RecyclerView;
import android.view.View;

public class RecyclerViewItemDecorator extends RecyclerView.ItemDecoration {

    private int mTopOffset;
    private int mBottomOffset;
    private int mLeftOffset;
    private int mRightOffset;

    public RecyclerViewItemDecorator(int top, int bottom, int left, int right) {
        mTopOffset = top;
        mBottomOffset = bottom;
        mLeftOffset = left;
        mRightOffset = right;
    }


    @Override
    public void getItemOffsets(Rect outRect, View view, RecyclerView parent,
                               RecyclerView.State state) {
        outRect.set(mLeftOffset, mTopOffset, mRightOffset, mBottomOffset);
    }
}