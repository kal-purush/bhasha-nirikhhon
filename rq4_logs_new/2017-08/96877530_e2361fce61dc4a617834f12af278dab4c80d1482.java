package com.fei.root.recater.decoration;

import android.graphics.Canvas;
import android.graphics.Paint;
import android.graphics.Rect;
import android.support.annotation.ColorInt;
import android.support.v7.widget.RecyclerView;
import android.view.View;

/**
 * Created by PengFeifei on 17-7-31.
 */

public class DividerDecoration extends RecyclerView.ItemDecoration {

    private int height=1;
    private Paint paint;

    public DividerDecoration(@ColorInt int color, int height) {
        paint = new Paint();
        paint.setColor(color);
        this.height = height;
    }

    public DividerDecoration(@ColorInt int color) {
        paint = new Paint();
        paint.setColor(color);
    }

    @Override
    public void getItemOffsets(Rect outRect, View view, RecyclerView parent, RecyclerView.State state) {
        super.getItemOffsets(outRect, view, parent, state);
        outRect.bottom = height;
    }

    @Override
    public void onDraw(Canvas canvas, RecyclerView parent, RecyclerView.State state) {
        int childCount = parent.getChildCount();
        int left = parent.getPaddingLeft();
        int right = parent.getWidth() - parent.getPaddingRight();
        for (int i = 0; i < childCount - 1; i++) {
            View view = parent.getChildAt(i);
            float top = view.getBottom();
            float bottom = view.getBottom() + height;
            canvas.drawRect(left, top, right, bottom, paint);
        }
    }

}