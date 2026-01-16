package com.zhao.recyclerviewtest;

import android.graphics.drawable.Drawable;
import android.support.v7.widget.LinearLayoutManager;
import android.support.v7.widget.RecyclerView;

/**
 * Created by Administrator on 2017/2/17.
 */

public class DividerItemDectation extends RecyclerView.ItemDecoration {
    private static final int[] ATTRS=new int[]{
            android.R.attr.listDivider
    };
    public static final int HORIZONTAL_LIST= LinearLayoutManager.HORIZONTAL;
    public static final int VERTICAL_LIST=LinearLayoutManager.VERTICAL;
    private Drawable drawable;
    private int mOrientation;
}