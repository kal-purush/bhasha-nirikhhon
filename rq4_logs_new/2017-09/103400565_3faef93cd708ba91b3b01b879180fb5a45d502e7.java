package com.hencoder.hencoderpracticedraw6.practice_draw;

import android.content.Context;
import android.graphics.Canvas;
import android.graphics.Color;
import android.graphics.Paint;
import android.graphics.RectF;
import android.support.annotation.Nullable;
import android.util.AttributeSet;

import com.hencoder.hencoderpracticedraw6.Utils;


/**
 * Created by Administrator on 2017/9/14.
 */

public class CustomArcView extends android.view.View {

    Paint mPaint = new Paint();
    Paint textPaint = new Paint();
    RectF mRectF = new RectF();
    float radius = Utils.dpToPixel(40);
    float progress = 30;

    public CustomArcView(Context context) {
        super(context);
    }

    public CustomArcView(Context context, @Nullable AttributeSet attrs) {
        super(context, attrs);
    }

    @Override
    protected void onDraw(Canvas canvas) {
        super.onDraw(canvas);
        mPaint.setStrokeWidth(Utils.dpToPixel(5));
        mPaint.setStrokeCap(Paint.Cap.ROUND);
        mPaint.setColor(Color.RED);
        mPaint.setAntiAlias(true);
        mPaint.setStyle(Paint.Style.STROKE);

        float centerX = getHeight()/2;
        float centerY = getWidth()/2;

        mRectF.top = centerY-radius;
        mRectF.left = centerX - radius;
        mRectF.right = centerX + radius;
        mRectF.bottom = centerY + radius;
        canvas.drawArc(mRectF,135,progress*2.7f,false,mPaint);

        textPaint.setTextSize(17);
        textPaint.setTextAlign(Paint.Align.CENTER);
        canvas.drawText(progress+"%",centerX,centerY - (textPaint.ascent() + textPaint.descent()) / 2,textPaint);

    }
}