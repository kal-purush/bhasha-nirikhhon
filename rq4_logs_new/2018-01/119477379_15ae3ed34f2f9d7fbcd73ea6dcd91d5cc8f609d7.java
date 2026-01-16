package two.isay.com.simpletext.scale;

import android.content.Context;
import android.support.v4.view.MotionEventCompat;
import android.support.v4.widget.ViewDragHelper;
import android.util.AttributeSet;
import android.view.GestureDetector;
import android.view.GestureDetector.OnDoubleTapListener;
import android.view.GestureDetector.OnGestureListener;
import android.view.MotionEvent;
import android.view.ScaleGestureDetector;
import android.view.ScaleGestureDetector.OnScaleGestureListener;
import android.view.View;
import android.widget.RelativeLayout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import two.isay.com.simpletext.R;

/**
 * 支持子view拖动
 * Created by cywu4 on 2017/12/8.
 */

public class VDHRelativeLayout extends RelativeLayout implements View.OnTouchListener {

    //是否捕获
    private boolean isCapture;
    //不需要捕获的id集合
    private List<Integer> mNoCaptureIds = new ArrayList<>();


    /**
     * 设置不支持拖动的子view。默认对所有子view支持拖动。
     *
     * @param ids 设置的id
     */
    public void setNoCaptureIds(List<Integer> ids) {
        if (ids != null) {
            mNoCaptureIds = ids;
        } else {
            mNoCaptureIds.clear();
        }
    }

    public VDHRelativeLayout(Context context) {
        this(context, null);
    }

    public VDHRelativeLayout(Context context, AttributeSet attrs) {
        super(context, attrs);
        setOnTouchListener(this);
    }

    @Override
    public boolean onInterceptTouchEvent(MotionEvent ev) {
        int action = MotionEventCompat.getActionMasked(ev);
        if (action == MotionEvent.ACTION_CANCEL || action == MotionEvent.ACTION_UP) {
            mVDH.cancel();
            return false;
        }
        return mVDH.shouldInterceptTouchEvent(ev);
    }

    @Override
    public boolean onTouch(View view, MotionEvent event) {
        mVDH.processTouchEvent(event);
        return isCapture;
    }

    private ViewDragHelper mVDH = ViewDragHelper.create(this, 1.0f, new ViewDragHelper.Callback() {
        @Override
        public boolean tryCaptureView(View child, int pointerId) {
            if (mNoCaptureIds.contains(child.getId())) {
                isCapture = false;
            } else {
                isCapture = true;
            }
            return isCapture;
        }

        @Override
        public int clampViewPositionHorizontal(View child, int left, int dx) {
            if (left < 0) {
                left = 0;
            } else if (left > getWidth() - child.getWidth()) {
                left = getWidth() - child.getWidth();
            }
            return left;
        }

        @Override
        public int clampViewPositionVertical(View child, int top, int dy) {
            if (top < 0) {
                top = 0;
            } else if (top > getHeight() - child.getHeight()) {
                top = getHeight() - child.getHeight();
            }
            return top;
        }

        @Override
        public int getViewHorizontalDragRange(View child) {
            return getMeasuredWidth() - child.getMeasuredWidth();
        }

        @Override
        public int getViewVerticalDragRange(View child) {
            return getMeasuredHeight() - child.getMeasuredHeight();
        }
    });

}