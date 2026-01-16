package sw19.moring03.paint.tools;

import android.graphics.Bitmap;
import android.graphics.Canvas;
import android.graphics.Color;
import android.graphics.Paint;
import android.graphics.PointF;
import android.graphics.Rect;

import java.util.ArrayList;

public class StickerTool extends Tools {

    private Bitmap iconToDraw;

    public StickerTool(Bitmap icon) {
        points = new ArrayList<>();
        strokeWidth = 5;
        color = Color.BLACK;
        iconToDraw = icon;
    }

    @Override
    public boolean draw(Canvas canvas, Paint paint) {
        if (points.size() != 1 || iconToDraw == null) {
            return false;
        }

        int x = (int)points.get(0).x;
        int y = (int)points.get(0).y;

        int iconWidth = iconToDraw.getWidth();
        int iconHeight = iconToDraw.getHeight();

        Rect src = new Rect(0, 0, iconWidth, iconHeight);
        Rect dst = new Rect(x - iconWidth / 2 , y - iconHeight / 2, x + iconWidth / 2, y + iconHeight / 2);

        canvas.drawBitmap(iconToDraw, src, dst, paint);

        return true;
    }

    @Override
    public void addPoint(PointF point) {
        if (this.getPoints().size() > 0) {
            return;
        }

        super.addPoint(point);
    }
}