package com.example.geekbrainsandroidweather.custom_views;

import android.content.Context;
import android.content.res.TypedArray;
import android.graphics.Canvas;
import android.graphics.Color;
import android.graphics.Paint;
import android.graphics.RectF;
import android.util.AttributeSet;
import android.view.View;

import com.example.geekbrainsandroidweather.R;

public class ThermometerView extends View {

    private float outerCircleRadius, outerRectRadius;
    private Paint outerPaint;
    private float middleCircleRadius, middleRectRadius;
    private Paint middlePaint;
    private float innerCircleRadius, innerRectRadius;
    private Paint innerPaint = new Paint();
    private int innerColor;
    private float maxTemp = 60;
    private float minTemp = -30;
    private float rangeTemp = 60;
    private float currentTemp;

    public ThermometerView(Context context) {
        super(context);
        init(context, null);
    }

    public ThermometerView(Context context, AttributeSet attrs) {
        super(context, attrs);
        init(context, attrs);
    }

    public ThermometerView(Context context, AttributeSet attrs, int defStyleAttr) {
        super(context, attrs, defStyleAttr);
        init(context, attrs);
    }

    public void setCurrentTemp(float currentTemp) {
        if (currentTemp > maxTemp) {
            this.currentTemp = maxTemp;
        } else if (currentTemp < minTemp) {
            this.currentTemp = minTemp;
        } else {
            this.currentTemp = currentTemp;
        }

        invalidate();
    }

    public float getMinTemp() {
        return minTemp;
    }

    public void init(Context context, AttributeSet attrs) {
        TypedArray typedArray = context.obtainStyledAttributes(attrs, R.styleable.Thermometer);
        outerCircleRadius = typedArray.getDimension(R.styleable.Thermometer_radius, 20f);
        int outerColor = typedArray.getColor(R.styleable.Thermometer_outerColor, Color.WHITE);
        int middleColor = typedArray.getColor(R.styleable.Thermometer_middleColor, Color.GRAY);
        innerColor = typedArray.getColor(R.styleable.Thermometer_innerColor, Color.RED);

        typedArray.recycle();

        outerRectRadius = outerCircleRadius / 2;
        outerPaint = new Paint();

        outerPaint.setColor(outerColor);
        outerPaint.setStyle(Paint.Style.FILL);

        middleCircleRadius = outerCircleRadius - 5;
        middleRectRadius = outerRectRadius - 5;
        middlePaint = new Paint();
        middlePaint.setColor(middleColor);
        middlePaint.setStyle(Paint.Style.FILL);

        innerCircleRadius = middleCircleRadius - middleCircleRadius / 6;
        innerRectRadius = middleRectRadius - middleRectRadius / 6;

        innerPaint.setColor(innerColor);
        innerPaint.setStyle(Paint.Style.FILL);
    }

    @Override
    protected void onDraw(Canvas canvas) {
        super.onDraw(canvas);

        if (currentTemp <= 0) {
            innerPaint.setColor(getResources().getColor(R.color.colorBlue));
        } else if (currentTemp > 0 && currentTemp < 30) {
            innerPaint.setColor(getResources().getColor(R.color.colorYellow));
        } else {
            innerPaint.setColor(innerColor);
        }
        innerPaint.setStyle(Paint.Style.FILL);

        int height = getHeight();
        int width = getWidth();

        int circleCenterX = width / 2;
        float circleCenterY = height - outerCircleRadius;
        float outerStartY = 0;
        float middleStartY = outerStartY + 5;


        float innerEffectStartY = middleStartY + middleRectRadius + 10;
        float innerEffectEndY = circleCenterY - outerCircleRadius - 10;
        float innerRectHeight = innerEffectEndY - innerEffectStartY;
        float innerStartY = innerEffectStartY + (maxTemp - currentTemp) / rangeTemp * innerRectHeight;

        RectF outerRect = new RectF();
        outerRect.left = circleCenterX - outerRectRadius;
        outerRect.top = outerStartY;
        outerRect.right = circleCenterX + outerRectRadius;
        outerRect.bottom = circleCenterY;

        canvas.drawRoundRect(outerRect, outerRectRadius, outerRectRadius, outerPaint);
        canvas.drawCircle(circleCenterX, circleCenterY, outerCircleRadius, outerPaint);

        RectF middleRect = new RectF();
        middleRect.left = circleCenterX - middleRectRadius;
        middleRect.top = middleStartY;
        middleRect.right = circleCenterX + middleRectRadius;
        middleRect.bottom = circleCenterY;

        canvas.drawRoundRect(middleRect, middleRectRadius, middleRectRadius, middlePaint);
        canvas.drawCircle(circleCenterX, circleCenterY, middleCircleRadius, middlePaint);

        canvas.drawRect(circleCenterX - innerRectRadius, innerStartY, circleCenterX + innerRectRadius, circleCenterY, innerPaint);
        canvas.drawCircle(circleCenterX, circleCenterY, innerCircleRadius, innerPaint);

    }

}