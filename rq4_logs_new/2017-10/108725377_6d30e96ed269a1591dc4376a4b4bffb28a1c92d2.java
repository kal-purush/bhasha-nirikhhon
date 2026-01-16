package qiuchenly.JSAHVCSC.Base;

import android.graphics.Bitmap;
import android.graphics.Canvas;
import android.graphics.Paint;
import android.graphics.PorterDuff;
import android.graphics.PorterDuffXfermode;
import android.graphics.Rect;
import android.graphics.RectF;

/**
 * Author        : chenyuqi
 * Times         : 2017 年 10 月 29 日 17:01
 * Usage         :
 */

public class BaseUtils {
    public static Bitmap makeRound(Bitmap bit,int size){
        int w = bit.getWidth();
        int h= bit.getHeight();
        Bitmap bits=Bitmap.createBitmap(w,h,Bitmap.Config.ARGB_8888);
        Canvas canvas = new Canvas(bits);
        int color=0xff424242;
        Paint paint = new Paint();
        Rect rect = new Rect(0,0,w,h);
        RectF rectF =new RectF(rect);
        paint.setAntiAlias(true);
        canvas.drawARGB(0,0,0,0);
        paint.setColor(color);
        canvas.drawRoundRect(rectF,size,size,paint);
        paint.setXfermode(new PorterDuffXfermode(PorterDuff.Mode.SRC_IN));
        canvas.drawBitmap(bit,rect,rect,paint);
        return bits;
    }
}