package com.music.cornell.music;

import android.content.Context;
import android.graphics.Canvas;
import android.graphics.Color;
import android.graphics.Paint;
import android.os.Build;
import android.support.annotation.RequiresApi;
import android.util.AttributeSet;
import android.view.SurfaceView;

/**
 * Created by dantech on 11/28/16.
 */

public class MapView extends SurfaceView {
    public MapView(Context context) {
        super(context);
    }

    public MapView(Context C, AttributeSet attribs){
        super(C, attribs);

        // Other setup code you want here
    }

    public MapView(Context C, AttributeSet attribs, int defStyle){
        super(C, attribs, defStyle);

        // Other setup code you want here
    }

    @RequiresApi(api = Build.VERSION_CODES.LOLLIPOP)
    protected void onDraw(Canvas canvas){
        super.onDraw(canvas);

        Paint textPaint = new Paint();
        textPaint.setColor(Color.RED);

        canvas.drawOval(40,40,100,100, textPaint);

    }
}