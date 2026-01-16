package com.group33.greenthumb;

import android.app.Activity;
import android.content.Intent;
import android.graphics.drawable.Drawable;
import android.os.Bundle;
import android.support.v4.view.GestureDetectorCompat;
import android.support.wearable.view.WatchViewStub;
import android.util.Log;
import android.view.GestureDetector;
import android.view.MotionEvent;
import android.view.View;
import android.widget.ImageButton;
import android.widget.ImageView;
import android.widget.TextView;

public class ViewTasksActivity extends Activity implements   GestureDetector.OnGestureListener,
        GestureDetector.OnDoubleTapListener, View.OnTouchListener {

    private TextView mTextView;
    String DEBUG_TAG = "View Task";
    private GestureDetectorCompat mDetector;
    private ViewTasksActivity x  = this;
    private static final int SWIPE_THRESHOLD = 0;
    private static final int SWIPE_VELOCITY_THRESHOLD = 0;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_view_tasks);
        final WatchViewStub stub = (WatchViewStub) findViewById(R.id.watch_view_stub);
        stub.setOnLayoutInflatedListener(new WatchViewStub.OnLayoutInflatedListener() {
            @Override
            public void onLayoutInflated(WatchViewStub stub) {
                mTextView = (TextView) stub.findViewById(R.id.text);

                ImageButton m = (ImageButton) findViewById(R.id.imageButton);
                m.setOnTouchListener(x);
                mDetector = new GestureDetectorCompat(x, x);
                // Set the gesture detector as the double tap
                // listener.
                mDetector.setOnDoubleTapListener(x);
            }
        });
    }

    @Override
    public boolean onTouchEvent(MotionEvent event){
        this.mDetector.onTouchEvent(event);
        // Be sure to call the superclass implementation
        return super.onTouchEvent(event);
    }

    @Override
    public boolean onDown(MotionEvent event) {
        Log.d(DEBUG_TAG, "onDown: " + event.toString());
        return true;
    }

    @Override
    public boolean onFling(MotionEvent event1, MotionEvent event2,
                           float velocityX, float velocityY) {
        Log.d(DEBUG_TAG, "onFling: " + event1.toString() + event2.toString());
        /*
        boolean result = false;
        float diffY = event2.getY() - event1.getY();
        float diffX = event2.getX() - event1.getX();
        if (Math.abs(diffX) > Math.abs(diffY)) {
            if (Math.abs(diffX) > SWIPE_THRESHOLD && Math.abs(velocityX) > SWIPE_VELOCITY_THRESHOLD) {
                if (diffX > 0) {
                    onSwipeRight();
                } else {
                    onSwipeLeft();
                }
                result = true;
            }
        }
        else if (Math.abs(diffY) > SWIPE_THRESHOLD && Math.abs(velocityY) > SWIPE_VELOCITY_THRESHOLD) {
            if (diffY > 0) {
                onSwipeDown();
            } else {
                onSwipeUp();
            }
            result = true;
        }
        return result;
        */
        return true;
    }

    public void onSwipeUp() {
        ImageButton m = (ImageButton) findViewById(R.id.imageButton);
        Drawable myDrawable = getDrawable(R.drawable.water_peonies);
        m.setImageDrawable(myDrawable);
    }

    public void onSwipeDown() {
        ImageButton m = (ImageButton) findViewById(R.id.imageButton);
        Drawable myDrawable = getDrawable(R.drawable.water_roses);
        m.setImageDrawable(myDrawable);
    }

    public void onSwipeRight() {
    }

    public void onSwipeLeft() {
    }

    @Override
    public void onLongPress(MotionEvent event) {
        Log.d(DEBUG_TAG, "onLongPress: " + event.toString());
    }

    @Override
    public boolean onScroll(MotionEvent e1, MotionEvent e2, float distanceX,
                            float distanceY) {
        Log.d(DEBUG_TAG, "onScroll: " + e1.toString()+e2.toString());
        return true;
    }

    @Override
    public void onShowPress(MotionEvent event) {
        Log.d(DEBUG_TAG, "onShowPress: " + event.toString());
    }

    @Override
    public boolean onSingleTapUp(MotionEvent event) {
        Log.d(DEBUG_TAG, "onSingleTapUp: " + event.toString());
        return true;
    }

    @Override
    public boolean onDoubleTap(MotionEvent event) {
        Log.d(DEBUG_TAG, "onDoubleTap: " + event.toString());
        ImageButton m = (ImageButton) findViewById(R.id.imageButton);
        Drawable myDrawable = getDrawable(R.drawable.water_roses);
        m.setImageDrawable(myDrawable);
        return true;
    }

    @Override
    public boolean onDoubleTapEvent(MotionEvent event) {
        Log.d(DEBUG_TAG, "onDoubleTapEvent: " + event.toString());
        return true;
    }

    @Override
    public boolean onSingleTapConfirmed(MotionEvent event) {
        Log.d(DEBUG_TAG, "onSingleTapConfirmed: " + event.toString());
        ImageButton m = (ImageButton) findViewById(R.id.imageButton);
        Drawable myDrawable = getDrawable(R.drawable.water_peonies);
        m.setImageDrawable(myDrawable);
        return true;
    }

    @Override
    public boolean onTouch(View v, MotionEvent event) {
        mDetector.onTouchEvent(event);
        return false;
    }
}