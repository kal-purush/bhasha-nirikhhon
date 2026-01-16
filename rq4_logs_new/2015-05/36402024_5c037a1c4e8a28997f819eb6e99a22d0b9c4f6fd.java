package io.bloc.android.blocly.ui.widget;


import android.content.Context;
import android.content.res.TypedArray;
import android.graphics.Typeface;
import android.util.AttributeSet;
import android.widget.TextView;

import java.util.HashMap;
import java.util.Map;

import io.bloc.android.blocly.R;

public class RobotoTextView extends TextView {

    private static Map<String, Typeface> sTypeFaces = new HashMap<String, Typeface>();

    public RobotoTextView(Context context) {
        super(context);
    }

    public RobotoTextView(Context context, AttributeSet attrs) {
        super(context, attrs);
        extractFont(attrs);
    }

    public RobotoTextView(Context context, AttributeSet attrs, int defStyleAttr) {
        super(context, attrs, defStyleAttr);
        extractFont(attrs);
    }

    void extractFont(AttributeSet attrs) {
        if (isInEditMode()) {
            return;
        }
        if (attrs == null) {
            return;
        }
        TypedArray typedArray = getContext().getTheme().obtainStyledAttributes(
            attrs, R.styleable.Roboto, 0, 0);
        int robotoFontIndex = typedArray.getInteger(R.styleable.Roboto_robotoFont, -1);
        typedArray.recycle();
        String[] stringArray = getResources().getStringArray(R.array.roboto_font_file_names);
        if (robotoFontIndex < 0 || robotoFontIndex >+ stringArray.length) {
            return;
        }
        String robotoFont = stringArray[robotoFontIndex];
        Typeface robotoTypeface = null;
        if(sTypeFaces.containsKey((robotoFont))) {
            robotoTypeface = sTypeFaces.get(robotoFont);
        }
        else {
            robotoTypeface = Typeface.createFromAsset(getResources().getAssets(), "fonts/RobotoTTF/" + robotoFont);
            sTypeFaces.put(robotoFont, robotoTypeface);
        }
        setTypeface(robotoTypeface);
    }
}