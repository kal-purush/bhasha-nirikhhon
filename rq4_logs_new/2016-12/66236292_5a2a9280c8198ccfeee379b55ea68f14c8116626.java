package com.onektower.customviews;

import android.content.Context;
import android.graphics.Canvas;
import android.graphics.Color;
import android.graphics.Paint;
import android.util.AttributeSet;
import android.view.View;

/**
 * Created by zhwilson on 2016/12/1.
 */
public class ClockView extends View implements Runnable{
    private static final double PI = Math.PI;
    private Context mContext;
    private int width;
    private int height;
    private int radius;
    private float lineLength;
    private float lineLengthdef;
    private int centerX, centerY;
    private float lineW;//线宽
    private Paint linePaint;
    private float currentDegree = 0;
    private float sectionDegree = 22.5f;

    public ClockView(Context context) {
        this(context, null);
    }

    public ClockView(Context context, AttributeSet attrs) {
        this(context, attrs, 0);
    }

    public ClockView(Context context, AttributeSet attrs, int defStyleAttr) {
        super(context, attrs, defStyleAttr);
        mContext = context;
        lineW = 2.0f;
        lineLengthdef = dp2px(10);
        initLinePaint();
        new Thread(this).start();
    }

    private void initLinePaint() {
        linePaint = new Paint(Paint.ANTI_ALIAS_FLAG);
        linePaint.setColor(Color.WHITE);
        linePaint.setStrokeWidth(lineW);
        linePaint.setStyle(Paint.Style.FILL_AND_STROKE);
    }

    @Override
    protected void onMeasure(int widthMeasureSpec, int heightMeasureSpec) {
        super.onMeasure(widthMeasureSpec, heightMeasureSpec);
    }

    @Override
    protected void onSizeChanged(int w, int h, int oldw, int oldh) {
        super.onSizeChanged(w, h, oldw, oldh);
        width = w;
        height = h;
        centerX = width / 2;
        centerY = height / 2;
        radius = width > height ? height / 2 - 120 : width / 2 - 120;
    }

    @Override
    protected void onDraw(Canvas canvas) {
        super.onDraw(canvas);
        canvas.drawColor(Color.BLACK);
        canvas.translate(centerX, centerY);
        for (float i = -22.5f; i < 382.5f; i += 3) {
            if (i >= currentDegree - sectionDegree && i <= currentDegree + sectionDegree) {
                float degree = i - (currentDegree - sectionDegree);
                lineLength = (float) (lineLengthdef * (1 + 1.1 * Math.sin(4*(degree/180)*PI)));
            } else {
                lineLength = lineLengthdef;
            }
            canvas.drawLine(-lineW / 2, -radius, -lineW / 2, -lineLength - radius, linePaint);
            canvas.rotate(3, 0, 0);
        }
        postInvalidate();
    }

    private float dp2px(float dp) {
        float scale = mContext.getResources().getDisplayMetrics().density;
        return scale * dp + 0.5f;
    }

    @Override
    public void run() {
        while (true){
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            currentDegree = currentDegree + 0.6f;
            if (currentDegree >= 360)
                currentDegree = 0;
        }
    }
}