package com.hunk.nobank.views;

import android.content.Context;
import android.graphics.Bitmap;
import android.graphics.BitmapShader;
import android.graphics.Canvas;
import android.graphics.Paint;
import android.graphics.Path;
import android.graphics.Shader;
import android.util.AttributeSet;
import android.view.MotionEvent;
import android.view.VelocityTracker;
import android.view.View;
import android.view.ViewGroup;
import android.widget.RelativeLayout;
import android.widget.Scroller;

import com.hunk.nobank.R;
import com.hunk.nobank.util.Logging;

public class SlideButton extends RelativeLayout {

    private static final int DURATION_TIME = 1 * 1000;

    private View mSlideIcon;

    private Scroller mScroller;
    private Integer mViewStartRawX;

    private int mScrollStartX;
    private OnTouchListener mSlideIconOnTouchListener = new OnTouchListener() {
        private float mStartX;
        private VelocityTracker mVelocityTracker;

        @Override
        public boolean onTouch(View v, MotionEvent event) {
            switch (event.getAction()) {
                case MotionEvent.ACTION_DOWN:
                    if (mViewStartRawX == null) {
                        mViewStartRawX = v.getLeft();
                    }
                    mStartX = event.getX();
                    Logging.d("ACTION_DOWN, and start x = " + mStartX);

                    if(mVelocityTracker == null) {
                        // Retrieve a new VelocityTracker object to watch the velocity of a motion.
                        mVelocityTracker = VelocityTracker.obtain();
                    } else {
                        // Reset the velocity tracker back to its initial state.
                        mVelocityTracker.clear();
                    }
                    // Add a user's movement to the tracker.
                    mVelocityTracker.addMovement(event);

                    break;
                case MotionEvent.ACTION_MOVE:
                    mVelocityTracker.addMovement(event);

                    int translateValue = (int)(event.getX() - mStartX);
                    Logging.d("ACTION_MOVE, translateValue = " + translateValue);
                    v.offsetLeftAndRight(translateValue);
                    postInvalidate();
                    break;
                case MotionEvent.ACTION_UP:
                    mVelocityTracker.recycle();
                    mVelocityTracker = null;
                    mScrollStartX = v.getLeft() - mViewStartRawX;
                    Logging.d("ACTION_UP, NOW = " + mScrollStartX);

                    mScroller = new Scroller(getContext());
                    if (mScrollStartX >= getMeasuredWidth()/2) {
                        Logging.d("ACTION_UP, reveal the view");
                        mScroller.startScroll(mScrollStartX, 0, getRight(), 0, DURATION_TIME);
                        postInvalidate();
                    } else {
                        Logging.d("ACTION_UP, cover the view : " + mScrollStartX);
                        mScroller.startScroll(mScrollStartX, 0, -mScrollStartX, 0, DURATION_TIME);
                        postInvalidate();
                    }
                    break;
            }
            return true;
        }
    };

    public SlideButton(Context context) {
        this(context, null);
    }

    public SlideButton(Context context, AttributeSet attrs) {
        this(context, attrs, 0);
    }

    public SlideButton(Context context, AttributeSet attrs, int defStyleAttr) {
        super(context, attrs, defStyleAttr);
        setWillNotDraw(false);
    }

    /**
     * So this will be called during parse layout xml.
     * @param child
     * @param index
     * @param params
     */
    @Override
    public void addView(View child, int index, ViewGroup.LayoutParams params) {
        super.addView(child, index, params);

        if (mSlideIcon == null && child.getId() == R.id.slide_icon) {
            mSlideIcon = child;
            mSlideIcon.setOnTouchListener(mSlideIconOnTouchListener);
        }
    }

    @Override
    protected void dispatchDraw(Canvas canvas) {
        super.dispatchDraw(canvas);
        Logging.d("draw mask");
        Bitmap bitmap = getMaskBitmap();
        drawMask(canvas, bitmap);
    }

    private Bitmap getMaskBitmap() {
        int lastChildIndex = 0;
        View realView = getChildAt(lastChildIndex);
        realView.setDrawingCacheEnabled(true);
        realView.buildDrawingCache();
        return realView.getDrawingCache();
    }

    /**
     * Draw the arrow and reveal picture for the mask.
     * @param canvas
     * @param bitmap
     */
    private void drawMask(Canvas canvas, Bitmap bitmap) {
        Paint mPaint = new Paint();
        mPaint.setColor(0xFFFFFFFF);
        mPaint.setStyle(Paint.Style.FILL);
        mPaint.setShader(new BitmapShader(bitmap, Shader.TileMode.REPEAT, Shader.TileMode.REPEAT));

        Path path = new Path();
        path.moveTo(0, 0);
        path.lineTo(mSlideIcon.getLeft(), mSlideIcon.getTop());
        path.lineTo(mSlideIcon.getRight(), mSlideIcon.getBottom() / 2);
        path.lineTo(mSlideIcon.getLeft(), mSlideIcon.getBottom());
        path.lineTo(0, mSlideIcon.getBottom());
        path.lineTo(0, 0);

        canvas.drawPath(path, mPaint);
    }

    @Override
    public void computeScroll() {
        if (mScroller != null) {
            if (mScroller.computeScrollOffset()) {
                Logging.d("mScroller.getFinalX()" + mScroller.getFinalX());
                Logging.d("getOffset = " + (mScroller.getCurrX() - mSlideIcon.getLeft()));
                mSlideIcon.offsetLeftAndRight(mScroller.getCurrX() - mSlideIcon.getLeft());
                postInvalidate();
            }
        }
    }
}