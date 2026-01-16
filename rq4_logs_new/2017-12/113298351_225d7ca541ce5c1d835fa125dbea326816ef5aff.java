package com.example.bao.mvpgank.widget;

import android.content.Context;
import android.support.v7.widget.AppCompatImageView;
import android.util.AttributeSet;

/**
 * Created by BAO on 2017/12/7.
 */

public class RatioImageview extends AppCompatImageView {
    private int originalWidth;
    private int originalHeight;
    public RatioImageview(Context context) {
        super(context);
    }

    public RatioImageview(Context context, AttributeSet attrs) {
        super(context, attrs);
    }

    public RatioImageview(Context context, AttributeSet attrs, int defStyleAttr) {
        super(context, attrs, defStyleAttr);
    }

    public void setOrigalSize(int originalWidth ,int originalHeight){
        this.originalWidth = originalWidth;
        this.originalHeight = originalHeight;
    }

    @Override
    protected void onMeasure(int widthMeasureSpec, int heightMeasureSpec) {
        if (originalWidth > 0 && originalHeight > 0){
            float ratio = (float) originalWidth/(float) originalHeight;
            int width = MeasureSpec.getSize(widthMeasureSpec);
            int height = MeasureSpec.getSize(heightMeasureSpec);
            // TODO: 现在只支持固定宽度
            if (width > 0) {
                height = (int) ((float) width / ratio);
            }

            setMeasuredDimension(width,height);
        }else {
            super.onMeasure(widthMeasureSpec, heightMeasureSpec);
        }


    }
}