package com.NumCo.numberconverter.ObjectPainter;

import android.graphics.Color;

public enum ObjectBitmapStatus {
    ERROR(Color.RED),
    NORMAL(Color.BLACK),
    DISABLED(Color.GRAY),
    ACTIVE_INPUT(Color.argb(255, 204, 139, 0)),
    ACTIVE_OUTPUT(Color.argb(255, 0, 0, 77));
    public int color;
    ObjectBitmapStatus(int color) {
        this.color = color;
    }
}