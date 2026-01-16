package drawField;

import android.content.Context;
import android.graphics.Canvas;
import android.graphics.Color;
import android.graphics.Paint;
import android.graphics.RectF;
import android.util.DisplayMetrics;
import android.view.MotionEvent;
import android.view.View;


public class Field extends View {

    private RectF[] robots;
    private float[] coord;
    private Paint roboPaint;
    private Paint textPaint;
    private float eX;
    private float eY;
    private int robotSelected;
    private float roboRadius;
    private String txt;



    public Field(Context context) {
        super(context);
        roboPaint = new Paint();
        roboPaint.setColor(Color.BLUE);
        textPaint = new Paint();


        robotSelected = -1;
        DisplayMetrics dispM = getContext().getResources().getDisplayMetrics();
        eY = (float)dispM.heightPixels / 480;
        eX = (float)dispM.widthPixels / 640;
        roboRadius = dispM.heightPixels / 20;
        txt = Float.toString(eX) + "\n" + Float.toString(eY);

        coord = new float[]{0, 100, 200, 1, 200, 300, 2, 300, 400, 3, 500, 400, 4, 500, 100};
        robots = new RectF[coord.length / 3];
        for(int i = 1; i < coord.length - 1; i+=3) {
            float left = coord[i] * eX - roboRadius;
            float top  = coord[i+1] * eY - roboRadius;
            robots[(i-1)/3] = new RectF(left, top, left + 2*roboRadius, top + 2*roboRadius);
        }


        setFocusableInTouchMode(true);
        setClickable(true);
        setLongClickable(false);
    }

    @Override
    public boolean onTouchEvent(MotionEvent event) {
        if (event.getAction() == MotionEvent.ACTION_DOWN) {
            float newX;
            float newY;
            newX = event.getX();
            newY = event.getY();
            for (int i = 0; i < robots.length; i++) {
                if(robots[i].contains(newX, newY)) {
                    robotSelected = i;
                    return true;
                }
            }
            if (robotSelected > -1) {
                robots[robotSelected].offsetTo(event.getX() - 50, event.getY() - 50);
                txt = Integer.toString(robotSelected);
                invalidate();
                return super.onTouchEvent(event);
            }
        }
        return false;
    }

    @Override
    public void onDraw(Canvas canvas) {
        super.onDraw(canvas);
        canvas.drawColor(Color.rgb(0, 139, 69));
        textPaint.setColor(Color.BLACK);
        textPaint.setTextSize(60);
        canvas.drawText(txt, 500, 500, textPaint);
        drawRobots(canvas);
    }

    private void drawRobots(Canvas canvas) {
        for (RectF r : robots) {
            canvas.drawRoundRect(r, 150, 150, roboPaint);
        }
    }
}
