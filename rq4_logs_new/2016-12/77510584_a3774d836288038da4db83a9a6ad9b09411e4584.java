package com.hankou.utils.views;

import android.content.Context;
import android.util.AttributeSet;
import android.util.Log;
import android.view.MotionEvent;
import android.widget.TextView;

/**
 * Created by bykj003 on 2016/12/5.
 */

public class TestTextView extends TextView{

    private static final String TAG = "TouchEvent";

    public TestTextView(Context context) {
        this(context,null);
    }

    public TestTextView(Context context, AttributeSet attrs) {
        this(context, attrs,0);
    }

    public TestTextView(Context context, AttributeSet attrs, int defStyleAttr) {
        super(context, attrs, defStyleAttr);
    }

    @Override
    public boolean dispatchTouchEvent(MotionEvent event) {
        int action = event.getAction();
        switch (action) {
            case MotionEvent.ACTION_DOWN:
                Log.i(TAG, "child down");
                break;
            case MotionEvent.ACTION_MOVE:
                Log.i(TAG, "child move");
                break;
            case MotionEvent.ACTION_UP:
                Log.i(TAG, "child up");
                break;
            case MotionEvent.ACTION_CANCEL:
                Log.i(TAG, "child cancel");
                break;
        }
        return true;
    }

    @Override
    public boolean onTouchEvent(MotionEvent event) {
        Log.i(TAG, "child touchevent");
        return super.onTouchEvent(event);
    }
}