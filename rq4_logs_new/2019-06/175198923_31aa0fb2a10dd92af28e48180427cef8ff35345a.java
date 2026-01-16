package sw19.moring03.paint.tools;

import android.graphics.Bitmap;
import android.graphics.Canvas;
import android.graphics.Color;
import android.graphics.Paint;
import android.graphics.Rect;

import java.util.ArrayList;

import sw19.moring03.paint.utils.PointF;

public class ChristmasTreeTool extends Tools {
    private Bitmap tree;

    public ChristmasTreeTool(Bitmap tree) {
        points = new ArrayList<>();
        strokeWidth = 5;
        color = Color.BLACK;
        this.tree = tree;
    }

    @Override
    public boolean draw(Canvas canvas, Paint paint) {
        if (points.size() != 2) {
            return false;
        }

        Rect destination = new Rect();

        if (points.get(0).x > points.get(1).x) {
            destination.right = (int)(points.get(0).x + Math.abs(getDistance() / 3));
            destination.left = (int)(points.get(1).x - Math.abs(getDistance() / 3));
        } else {
            destination.right = (int)(points.get(1).x + Math.abs(getDistance() / 3));
            destination.left = (int)(points.get(0).x - Math.abs(getDistance() / 3));
        }

        if (points.get(0).y > points.get(1).y) {
            destination.bottom = (int)points.get(0).y;
            destination.top = (int)points.get(1).y;
        } else {
            destination.bottom = (int)points.get(1).y;
            destination.top = (int)points.get(0).y;
        }

        canvas.drawBitmap(tree,null, destination, null);

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