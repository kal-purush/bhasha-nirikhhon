package com.example.pc_key.sipsesforever;

import android.graphics.Bitmap;
import android.graphics.Canvas;
import android.graphics.Paint;
import android.graphics.RectF;

/**
 * Created by Vadim on 22.06.2017.
 */

public class Bomb {

    float x, y;
    float vy;
    Paint paint = new Paint();
    Bitmap bomb;
    RectF dstBomb = new RectF();


    public Bomb(float x, float y, float vy, Bitmap bomb) {
        this.x = x;
        this.y = y;
        this.vy=vy;
        this.bomb = bomb;
    }

    public void draw(Canvas canvas) {
        dstBomb.set(x - bomb.getWidth() / 2, y - bomb.getHeight() / 2, x + bomb.getWidth() / 2, y + bomb.getHeight() / 2);
        canvas.drawBitmap(bomb, null, dstBomb, paint);
    }
}