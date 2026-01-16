package com.freud.mrzz.views;

import android.content.Context;
import android.util.AttributeSet;
import android.view.LayoutInflater;
import android.widget.RelativeLayout;

import com.freud.mrzz.R;


/**
 * TODO: document your custom view class.
 */
public class clothCategoriesView extends RelativeLayout {

    Context context;

    public clothCategoriesView(Context context) {
        super(context);
        init(context,null, 0);
    }

    public clothCategoriesView(Context context, AttributeSet attrs) {
        super(context, attrs);
        init(context,attrs, 0);
    }

    public clothCategoriesView(Context context, AttributeSet attrs, int defStyle) {
        super(context, attrs, defStyle);
        init(context,attrs, defStyle);
    }

    private void init(Context context,AttributeSet attrs, int defStyle) {
        LayoutInflater.from(context).inflate(R.layout.view_category_sub_cloth_frg, this);
        this.context=context;
    }


}