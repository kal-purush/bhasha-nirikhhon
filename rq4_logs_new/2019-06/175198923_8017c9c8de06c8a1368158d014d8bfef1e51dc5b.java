package sw19.moring03.paint.tools;

import android.graphics.Canvas;
import android.graphics.Color;
import android.graphics.Paint;
import android.graphics.Path;

import java.util.ArrayList;

import sw19.moring03.paint.utils.PointF;

public class HeartTool extends Tools {

    public HeartTool() {
        points = new ArrayList<>();
        strokeWidth = 5;
        color = Color.BLACK;
    }

    public HeartTool(int col, int strkW) {
        points = new ArrayList<>();
        strokeWidth = strkW;
        color = col;
    }

    @Override
    public boolean draw(Canvas canvas, Paint paint) {
        if (points.size() != 2) {
            return false;
        }

        float distance = (float)getDistance()/2;

        canvas.save();
        canvas.rotate(calculateRotation());

        PointF secondPoint = new PointF(points.get(0).x, points.get(0).y + distance*2);
        PointF thirdPoint = new PointF(points.get(0).x + distance,points.get(0).y + distance);
        PointF fourthPoint = new PointF(points.get(0).x - distance,points.get(0).y + distance);

        canvas.drawLine(points.get(0).x,points.get(0).y,thirdPoint.x, thirdPoint.y, paint);
        canvas.drawLine(points.get(0).x,points.get(0).y,fourthPoint.x, fourthPoint.y, paint);

        PointF tmp = new PointF((secondPoint.x + thirdPoint.x)/2,(secondPoint.y + thirdPoint.y)/2);

        float dist = (float)Math.sqrt(Math.pow(thirdPoint.x - secondPoint.x,2) + Math.pow(thirdPoint.y - secondPoint.y,2));
        canvas.drawArc(tmp.x - dist/2,tmp.y + dist/2,tmp.x + dist/2,tmp.y - dist/2, 0, 50, true, paint);
        tmp = new PointF((secondPoint.x + fourthPoint.x)/2,(secondPoint.y + fourthPoint.y)/2);
        canvas.drawArc(tmp.x - dist/2,tmp.y + dist/2,tmp.x + dist/2,tmp.y - dist/2, 0, 360, true, paint);

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

        if (firstPoint.y > secondPoint.y) {
            return - Math.sqrt(Math.pow(firstPoint.x - secondPoint.x,2) + Math.pow(firstPoint.y - secondPoint.y,2));
        } else {
            return Math.sqrt(Math.pow(firstPoint.x - secondPoint.x,2) + Math.pow(firstPoint.y - secondPoint.y,2));
        }
    }

    private float calculateRotation() {

        double distance = getDistance();

        double tmp = points.get(0).y - points.get(1).y;

        double angle = Math.asin(tmp/distance);

        return (float)angle;
    }
}