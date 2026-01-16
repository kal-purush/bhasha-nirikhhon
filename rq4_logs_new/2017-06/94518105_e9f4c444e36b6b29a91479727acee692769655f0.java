package com.example.pc_key.sipsesforever;

import android.graphics.Bitmap;
import android.graphics.Canvas;
import android.graphics.Paint;
import android.graphics.RectF;

/**
 * Created by PC_key on 19.06.2017.
 */

public class Ball {
    Paint paint = new Paint();
    float x, y;
    Bitmap ball;
    RectF dstBall = new RectF();

    public Ball(float x, float y, Bitmap ball) {
        this.x = x;
        this.y = y;
        this.ball = ball;
    }

    public void draw(Canvas canvas) {
        dstBall.set(x - ball.getWidth()/2, y - ball.getHeight()/2,
                x + ball.getWidth()/2, y + ball.getHeight()/2);
        canvas.drawBitmap(ball, null, dstBall, paint);
    }
}