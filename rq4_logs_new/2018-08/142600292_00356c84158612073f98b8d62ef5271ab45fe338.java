package com.proyectotec.kevin.proyectotec;

import android.content.Context;
import android.graphics.Typeface;
import android.support.v7.widget.AppCompatTextView;
import android.util.AttributeSet;

public class IconFont extends AppCompatTextView {
    public IconFont(Context context, AttributeSet attrs, int defStyle) {
        super(context, attrs, defStyle);
        setType(context);
    }

    public IconFont(Context context, AttributeSet attrs) {
        super(context, attrs);
        setType(context);
    }

    public IconFont(Context context) {
        super(context);
        setType(context);
    }

    private void setType(Context context){
        this.setTypeface(Typeface.createFromAsset(context.getAssets(),
                "fonts/fontawesome-webfont.ttf"));

    }
}