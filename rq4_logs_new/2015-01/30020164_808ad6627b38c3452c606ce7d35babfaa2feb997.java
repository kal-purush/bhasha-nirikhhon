package com.l3.android.ccbuptservice;

import android.content.Context;

/**
 * Created by Ihsan on 15/1/30.
 */
public class DisplayUtil {
    public static int dp2px(Context context, float dpValue) {
        final float scale = context.getResources().getDisplayMetrics().density;
        return (int) (dpValue * scale + 0.5f);
    }
}