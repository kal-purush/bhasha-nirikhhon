package com.mobile.piechart.views;

import android.Manifest;
import android.content.Context;
import android.content.pm.PackageManager;
import android.graphics.Canvas;
import android.graphics.Color;
import android.graphics.Paint;
import android.graphics.RectF;
import android.os.Build;
import android.os.Vibrator;
import android.support.v4.content.ContextCompat;
import android.util.AttributeSet;
import android.view.MotionEvent;
import android.view.View;

/**
 * @author David Castillo Fuentes
 * This is an implementation of a Time Selector, in which you can set a given amount of time by
 * adding minutes when you dial the selector in clockwise and removing minutes by dialing in the
 * opposite side.
 *
 * If the Vibrator is available in your device & proper permissions are added in your manifest
 * file and granted by the user, it will vibrate for each minute you add/remove from the selector
 */
public class DialView extends View {
    private static final String LOG = "DialView";
    private static final int MIN_ANGLE_REQUIRED = -90;
    private static final int DEFAULT_ALPHA_VALUE = 255;
    private static final int DEFAULT_COLOR = Color.BLACK;
    private static final int INVALID_PROGRESS_VALUE = -1;
    private static final int MINUTE_VALUE_TO_DEGREES_STEP_SIZE = 6;

    private static float BASE_STROKE_WIDTH_PERCENTAGE = 0.01f;

    private Paint mPaint1;
    private Paint mPaint2;

    private float mBaseSize;

    private int mLineColorStrong = DEFAULT_COLOR;
    private int mLineColorLight = DEFAULT_COLOR;
    private int mLineColorBlue = DEFAULT_COLOR;

    private float centerX;
    private float centerY;

    private float mMinutesRadio;
    private float mCircle1Radio;
    private float mCircle2Radio;
    private float mCircle3Radio;

    private final int[] minutes = {30, 45, 60, 15};

    private long mCurrentAngle = MIN_ANGLE_REQUIRED;
    private long mCurrentTime;
    private long mVibrator1;
    private long mVibrator2;

    private boolean mVibratorPermissionEnabled;

    private float mTouchIgnoreRadius;
    private double mTouchAngle;

    private int mAngle1;
    private int mAngle2;
    private int mAngle;
    private int mDiff;

    private OnDialViewChangeListener mOnDialViewChangeListener;
    private Vibrator mVibrator;

    public interface OnDialViewChangeListener {

        /**
         * Client can be notified when the progress level has changed.
         *
         * @param dialView
         *            The SeekArc whose progress has changed
         * @param progress
         *            The current progress time. This will be reported in seconds
         */
        void onProgressChanged(DialView dialView, long progress);

    }

    public DialView(Context context) {
        super(context);
        init(null, 0);
    }

    public DialView(Context context, AttributeSet attrs) {
        super(context, attrs);
        init(attrs, 0);
    }

    public DialView(Context context, AttributeSet attrs, int defStyle) {
        super(context, attrs, defStyle);
        init(attrs, defStyle);
    }

    private void init(AttributeSet attrs, int defStyle) {

        if(Build.VERSION.SDK_INT >= Build.VERSION_CODES.HONEYCOMB) {
            setLayerType(LAYER_TYPE_SOFTWARE, null);
        }

        mPaint1 = new Paint();
        mPaint1.setAntiAlias(true);
        mPaint1.setStyle(Paint.Style.FILL_AND_STROKE);
        mPaint1.setTextAlign(Paint.Align.CENTER);
        mPaint1.setTextSize(40);

        mPaint2 = new Paint();
        mPaint2.setAntiAlias(true);

        mLineColorStrong = Color.argb(DEFAULT_ALPHA_VALUE, 164, 164, 164);
        mLineColorLight = Color.argb(DEFAULT_ALPHA_VALUE, 187, 187, 187);
        mLineColorBlue = Color.argb(DEFAULT_ALPHA_VALUE, 21, 133, 216);

        int permissionCheck = ContextCompat.checkSelfPermission(getContext(),
                Manifest.permission.VIBRATE);
        mVibratorPermissionEnabled = permissionCheck == PackageManager.PERMISSION_GRANTED;
        if (mVibratorPermissionEnabled) {
            mVibrator = (Vibrator) getContext().getSystemService(Context.VIBRATOR_SERVICE);
        }
    }

    @Override
    protected void onDraw(Canvas canvas) {
        super.onDraw(canvas);

        Point point1;
        Point point2;
        int index = 0;
        float baseStrokeWidth = BASE_STROKE_WIDTH_PERCENTAGE * mBaseSize;

        // Draw Texts & Lines for Minutes
        for (int i = 1; i <= 60; i++) {

            if ((i % 15) == 0) {

                point1 = buildCoordinateXY(mMinutesRadio, centerX, centerY,
                        MINUTE_VALUE_TO_DEGREES_STEP_SIZE * i);

                // Draw Time
                mPaint1.setStrokeWidth(0.3f * baseStrokeWidth);
                mPaint1.setColor(Color.argb(DEFAULT_ALPHA_VALUE, 138, 138, 138));
                canvas.drawText(Integer.toString(minutes[index++]), point1.x, point1.y, mPaint1);

                point1 = buildCoordinateXY(mCircle1Radio, centerX, centerY,
                        MINUTE_VALUE_TO_DEGREES_STEP_SIZE * i);

                // Draw Line
                mPaint1.setStrokeWidth(baseStrokeWidth);
                mPaint1.setColor(mLineColorStrong);
            } else {

                point1 = buildCoordinateXY(mCircle1Radio, centerX, centerY,
                        MINUTE_VALUE_TO_DEGREES_STEP_SIZE * i);

                // Draw Line
                mPaint1.setStrokeWidth(0.8f * baseStrokeWidth);
                mPaint1.setColor(mLineColorLight);
            }
            canvas.drawLine(centerX, centerY, point1.x, point1.y, mPaint1);
        }

        // Draw Circle 2
        mPaint1.setStrokeWidth(baseStrokeWidth);
        mPaint1.setColor(Color.WHITE);
        canvas.drawCircle(centerX, centerY, mCircle2Radio, mPaint1);

        // Draw Circle 3
        mPaint2.setStrokeWidth(baseStrokeWidth);
        mPaint2.setColor(Color.argb(DEFAULT_ALPHA_VALUE, 234, 234, 234));
        canvas.drawCircle(centerX, centerY, mCircle3Radio, mPaint2);

        // Draw Minutes Indicator
        mPaint1.setColor(mLineColorBlue);
        point1 = buildCoordinateXY(0.85f * mCircle3Radio, centerX, centerY, (mCurrentAngle /
                MINUTE_VALUE_TO_DEGREES_STEP_SIZE) * MINUTE_VALUE_TO_DEGREES_STEP_SIZE);
        point2 = buildCoordinateXY(0.95f * mCircle3Radio, centerX, centerY, (mCurrentAngle /
                MINUTE_VALUE_TO_DEGREES_STEP_SIZE) * MINUTE_VALUE_TO_DEGREES_STEP_SIZE);
        canvas.drawLine(point1.x, point1.y, point2.x, point2.y, mPaint1);

    }

    // Measure the custom view to the specified size
    public void onMeasure(int widthMeasureSpec, int heightMeasureSpec) {

        int widthMode = MeasureSpec.getMode(widthMeasureSpec);
        int widthSize = MeasureSpec.getSize(widthMeasureSpec);
        int heightMode = MeasureSpec.getMode(heightMeasureSpec);
        int heightSize = MeasureSpec.getSize(heightMeasureSpec);

        int width;
        int height;

        // Measure Width
        if (widthMode == MeasureSpec.EXACTLY) {
            // Must be this size
            width = widthSize;
        } else if (widthMode == MeasureSpec.AT_MOST) {
            // Can't be bigger than...
            width = widthSize;
        } else {
            // Be whatever you want
            width = widthSize;
        }

        // Measure Height
        if (heightMode == MeasureSpec.EXACTLY) {
            // Must be this size
            height = heightSize;
        } else if (heightMode == MeasureSpec.AT_MOST) {
            // Can't be bigger than...
            height = heightSize;
        } else {
            // Be whatever you want
            height = heightSize;
        }

        // MUST CALL THIS
        setMeasuredDimension(width, height);
    }

    @Override
    protected void onLayout(boolean changed, int left, int top, int right, int bottom) {
        super.onLayout(changed, left, top, right, bottom);

        // If there is no change, skip the calculation
        if (!changed) return;

        // Make all the calculations
        int paddingLeft = getPaddingLeft();
        int paddingTop = getPaddingTop();
        int paddingRight = getPaddingRight();
        int paddingBottom = getPaddingBottom();

        int contentWidth = getWidth() - paddingLeft - paddingRight;
        int contentHeight = getHeight() - paddingTop - paddingBottom;

        mBaseSize = Math.min(contentWidth, contentHeight);

        // Calculate the base top, left, button, right
        float mLeft;
        float mTop;
        float mRight;
        float mBottom;
        float padding = BASE_STROKE_WIDTH_PERCENTAGE * mBaseSize;

        if (contentHeight > contentWidth) {
            mLeft = padding / 2f;
            mTop = (contentHeight - contentWidth + padding) / 2f;
            mRight = contentWidth - padding / 2f;
            mBottom = contentHeight - mTop;
        } else if (contentWidth > contentHeight) {
            mLeft = (contentWidth - contentHeight + padding) / 2f;
            mTop = padding / 2f;
            mRight = (contentWidth + contentHeight - padding) / 2f;
            mBottom = contentHeight - padding / 2f;
        } else {
            mLeft = padding / 2f;
            mTop = padding / 2f;
            mRight = contentWidth - padding / 2f;
            mBottom = contentHeight - padding / 2f;
        }

        // Measure & Position Minutes Rectangle
        RectF rectMinutes= new RectF();
        rectMinutes.left = mLeft + paddingLeft;
        rectMinutes.top = mTop + paddingTop;
        rectMinutes.right = mRight + paddingRight;
        rectMinutes.bottom = mBottom + paddingBottom;

        mMinutesRadio = Math.min(rectMinutes.width() / 2f, rectMinutes.height() / 2f);

        // Measure & Position Circle 1
        RectF rectCircle1 = new RectF();
        rectCircle1.left = rectMinutes.left + 4f * padding;
        rectCircle1.top = rectMinutes.top + 4f * padding;
        rectCircle1.right = rectMinutes.right - 4f * padding;
        rectCircle1.bottom = rectMinutes.bottom - 4f * padding;

        mCircle1Radio = Math.min(rectCircle1.width() / 2f, rectCircle1.height() / 2f);

        // Measure & Position Circle 2
        RectF rectCircle2 = new RectF();
        rectCircle2.left = rectMinutes.left + 8 * padding;
        rectCircle2.top = rectMinutes.top + 8 * padding;
        rectCircle2.right = rectMinutes.right - 8 * padding;
        rectCircle2.bottom = rectMinutes.bottom - 8 * padding;

        mCircle2Radio = Math.min(rectCircle2.width() / 2f, rectCircle2.height() / 2f);

        RectF rectCircle3 = new RectF();
        rectCircle3.left = rectMinutes.left + 10 * padding;
        rectCircle3.top = rectMinutes.top + 10 * padding;
        rectCircle3.right = rectMinutes.right - 10 * padding;
        rectCircle3.bottom = rectMinutes.bottom - 10 * padding;

        mCircle3Radio = Math.min(rectCircle3.width() / 2f, rectCircle3.height() / 2f);


        mPaint2.setShadowLayer(BASE_STROKE_WIDTH_PERCENTAGE * mBaseSize, 0.0f, 2.0f, Color.BLACK);

        // Calculate the center of the view
        centerX = getWidth() / 2f;
        centerY = getHeight() / 2f;

        setTouchInSide();
    }

    @Override
    public boolean onTouchEvent(MotionEvent event) {
        getParent().requestDisallowInterceptTouchEvent(true);

        switch (event.getAction()) {
            case MotionEvent.ACTION_DOWN:
                mAngle = updateOnTouch(event);
                if (mAngle != INVALID_PROGRESS_VALUE) {
                    mAngle1 = mAngle;
                }
                mVibrator1 = mVibrator2 = 0;

                break;
            case MotionEvent.ACTION_MOVE:
                mAngle = updateOnTouch(event);
                if (mAngle != INVALID_PROGRESS_VALUE) {
                    mAngle2 = mAngle;

                    if (isIn1stQuadrant(mAngle1) && isIn4thQuadrant(mAngle2)) {
                        mDiff = -(mAngle1 + (360 - mAngle2));
                    } else if (isIn1stQuadrant(mAngle2) && isIn4thQuadrant(mAngle1)) {
                        mDiff = mAngle2 + (360 - mAngle1);
                    } else {
                        mDiff = mAngle2 - mAngle1;
                    }
                    mAngle1 = mAngle2;

                    // Calculate Total Angle
                    mCurrentAngle += mDiff;
                    if (mCurrentAngle < MIN_ANGLE_REQUIRED) {
                        mCurrentAngle = MIN_ANGLE_REQUIRED;
                    } else if (mVibrator != null && mVibrator.hasVibrator()) {
                        mVibrator2 = mCurrentAngle / MINUTE_VALUE_TO_DEGREES_STEP_SIZE;
                        if (mVibrator1 != mVibrator2) {
                            mVibrator.vibrate(50);
                            mVibrator1 = mVibrator2;
                        }
                    }

                    // Calculate current time in seconds
                    mCurrentTime = calculateCurrentTime();

                    // Update UI
                    invalidate();

                    // Update time to the Client App
                    if (mOnDialViewChangeListener != null) {
                        mOnDialViewChangeListener.onProgressChanged(this, mCurrentTime);
                    }
                }

                break;
            case MotionEvent.ACTION_UP:
            case MotionEvent.ACTION_CANCEL:
                setPressed(false);
                getParent().requestDisallowInterceptTouchEvent(false);
                if (mVibrator != null && mVibrator.hasVibrator()) {
                    mVibrator.cancel();
                }
                break;
        }
        return true;
    }

    private int updateOnTouch(MotionEvent event) {
        boolean ignoreTouch = ignoreTouch(event.getX(), event.getY());
        if (ignoreTouch) return INVALID_PROGRESS_VALUE;
        setPressed(true);
        mTouchAngle = getTouchDegrees(event.getX(), event.getY());
        int progress = getProgressForAngle(mTouchAngle);
        return progress;
    }

    private boolean isIn1stQuadrant(double angle) {
        return angle >= 0 && angle <= 90;
    }

    private boolean isIn4thQuadrant(double angle) {
        return angle >= 270 && angle <= 360;
    }

    private int getProgressForAngle(double angle) {
        int touchProgress = (int) Math.round(angle);

        touchProgress = (touchProgress < 0) ? INVALID_PROGRESS_VALUE
                : touchProgress;
        touchProgress = (touchProgress > 360) ? INVALID_PROGRESS_VALUE
                : touchProgress;

        return touchProgress;
    }

    private boolean ignoreTouch(float xPos, float yPos) {
        boolean ignore = false;
        float x = xPos - centerX;
        float y = yPos - centerY;

        float touchRadius = (float) Math.sqrt(((x * x) + (y * y)));
        if (touchRadius < mTouchIgnoreRadius) {
            ignore = true;
        }
        return ignore;
    }

    public void setTouchInSide() {
        mTouchIgnoreRadius = mCircle3Radio / 4;
    }

    private double getTouchDegrees(float xPos, float yPos) {
        float x = xPos - centerX;
        float y = yPos - centerY;
        double angle = Math.toDegrees(Math.atan2(y, x) + (Math.PI / 2));

        if (angle < 0) {
            angle = 360 + angle;
        }

        return angle;
    }

    public long getCurrentTime() {
        return mCurrentTime;
    }

    public void setCurrentTime(long currentTime) {
        this.mCurrentTime = currentTime;
        this.mCurrentAngle = calculateCurrentAngle(currentTime);
        this.invalidate();
    }

    public long getCurrentAngle() {
        return mCurrentAngle;
    }

    private long calculateCurrentTime() {
        return 10 * (mCurrentAngle + 90);
    }

    private long calculateCurrentAngle(long currentTime) {
        return currentTime / 10 - 90;
    }

    public OnDialViewChangeListener getOnDialViewChangeListener() {
        return mOnDialViewChangeListener;
    }

    public void setOnDialViewChangeListener(OnDialViewChangeListener dialViewChangeListener) {
        this.mOnDialViewChangeListener = dialViewChangeListener;
    }

    private Point buildCoordinateXY(float radio, float centerX, float centerY, float
            angleInDegrees) {
        Point point = new Point();
        point.x = centerX + (float)(radio * Math.cos(Math.toRadians(angleInDegrees)));
        point.y = centerY + (float)(radio * Math.sin(Math.toRadians(angleInDegrees)));
        return point;
    }

    public static class Point {
        float x;
        float y;
    }

}