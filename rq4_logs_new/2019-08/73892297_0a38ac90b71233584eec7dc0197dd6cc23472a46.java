package com.swein.framework.template.customviewcontroller;

import android.content.Context;
import android.graphics.Canvas;
import android.graphics.Rect;
import android.support.annotation.NonNull;
import android.support.annotation.Nullable;
import android.util.AttributeSet;
import android.view.LayoutInflater;
import android.view.View;
import android.widget.FrameLayout;
import android.widget.TextView;

import com.swein.framework.tools.util.debug.log.ILog;
import com.swein.shandroidtoolutils.R;

public class NavigationViewController extends FrameLayout {

    private final static String TAG = "NavigationViewController";

    private TextView textViewTitle;
    private String title;

    public NavigationViewController(@NonNull Context context) {
        super(context);

        LayoutInflater.from(context).inflate(R.layout.view_controller_navigation, this);
        textViewTitle = findViewById(R.id.textViewTitle);
    }


    @Override
    protected void onFinishInflate() {
        super.onFinishInflate();

    }

    @Override
    protected void onAttachedToWindow() {
        super.onAttachedToWindow();
        ILog.iLogDebug(TAG, "onAttachedToWindow");

        textViewTitle.setText(title);
    }

    @Override
    protected void onMeasure(int widthMeasureSpec, int heightMeasureSpec) {
        super.onMeasure(widthMeasureSpec, heightMeasureSpec);
        ILog.iLogDebug(TAG, "onMeasure");
    }

    @Override
    protected void onSizeChanged(int w, int h, int oldw, int oldh) {
        super.onSizeChanged(w, h, oldw, oldh);
        ILog.iLogDebug(TAG, "onSizeChanged");
    }

    @Override
    protected void onLayout(boolean changed, int left, int top, int right, int bottom) {
        super.onLayout(changed, left, top, right, bottom);
        ILog.iLogDebug(TAG, "onLayout");

    }

    @Override
    protected void onDraw(Canvas canvas) {
        super.onDraw(canvas);
        ILog.iLogDebug(TAG, "onDraw");

    }

    @Override
    protected void onFocusChanged(boolean gainFocus, int direction, @Nullable Rect previouslyFocusedRect) {
        super.onFocusChanged(gainFocus, direction, previouslyFocusedRect);
        ILog.iLogDebug(TAG, "onFocusChanged");
    }

    @Override
    public void onWindowFocusChanged(boolean hasWindowFocus) {
        super.onWindowFocusChanged(hasWindowFocus);
        ILog.iLogDebug(TAG, "onWindowFocusChanged");
    }

    @Override
    protected void onVisibilityChanged(@NonNull View changedView, int visibility) {
        super.onVisibilityChanged(changedView, visibility);
        ILog.iLogDebug(TAG, "onVisibilityChanged");
    }

    @Override
    protected void onWindowVisibilityChanged(int visibility) {
        ILog.iLogDebug(TAG, "onWindowVisibilityChanged");
        super.onWindowVisibilityChanged(visibility);
    }

    @Override
    protected void onDetachedFromWindow() {
        ILog.iLogDebug(TAG, "onDetachedFromWindow");
        super.onDetachedFromWindow();
    }

    @Override
    protected void finalize() throws Throwable {
        ILog.iLogDebug(TAG, "finalize");
        super.finalize();
    }

    public void setTitle(String title) {
        this.title = title;
    }
}