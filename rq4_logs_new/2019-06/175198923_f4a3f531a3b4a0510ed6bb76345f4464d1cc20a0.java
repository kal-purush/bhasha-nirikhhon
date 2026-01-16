package sw19.moring03.paint.tools;

import android.graphics.Canvas;
import android.graphics.Color;
import android.graphics.Paint;
import android.graphics.Path;

import java.util.ArrayList;

import sw19.moring03.paint.utils.PointF;

public class StarTool extends Tools {
    private final float CORNERS = 5.0f;
    private final float GOLDENRATIO = 1.618f;

    public StarTool() {
        points = new ArrayList<>();
        strokeWidth = 5;
        color = Color.BLACK;
    }

    public StarTool(int col, int strkW) {
        points = new ArrayList<>();
        strokeWidth = strkW;
        color = col;
    }

    @Override
    public boolean draw(Canvas canvas, Paint paint) {
        if (points.size() != 2) {
            return false;
        }

        float halfDist = (float)getDistance() / 2;

        PointF tmp = new PointF(points.get(0).x + halfDist, points.get(0).y);
        //https://www.mathsisfun.com/geometry/pentagram.html
        float b = (float)getDistance() / GOLDENRATIO;
        float c = b / GOLDENRATIO;
        float d = c / GOLDENRATIO;
        float x = (float)Math.sqrt(Math.pow(c, 2) - Math.pow(d/2,2));

        canvas.save();
        for (int corners = 1; corners <= CORNERS; corners++) {
            canvas.translate(halfDist, -x);
            canvas.rotate(360/CORNERS, points.get(0).x, points.get(0).y);
            canvas.drawLine(points.get(0).x, points.get(0).y,tmp.x + halfDist, tmp.y,paint);
        }
        canvas.restore();

        return true;
    }

    @Override
    public void addPoint(PointF point) {
        if (points.size() == 2) {
            points.remove(1);
        }

        super.addPoint(point);
    }

    private double getDistance() {
        PointF firstPoint = points.get(0);
        PointF secondPoint = points.get(1);

        return Math.sqrt(Math.pow(firstPoint.x - secondPoint.x,2) + Math.pow(firstPoint.y - secondPoint.y,2));

    }

}