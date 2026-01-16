package com.example.gav.taskmanager.main;

import android.content.Context;
import android.content.res.TypedArray;

import com.example.gav.taskmanager.R;

public final class ResourcesHelper {
    public static int[] getColorArray(Context context, int id) {
        TypedArray colorTypedArray = context.getResources().obtainTypedArray(id);
        int[] result = new int[colorTypedArray.length()];
        for (int i = 0; i < colorTypedArray.length(); i++) {
            result[i] = colorTypedArray.getColor(i, 0);
        }
        colorTypedArray.recycle();
        return result;
    }
}