package com.example.drawtest;

import android.content.Context;
import android.graphics.Canvas;
import android.graphics.Color;
import android.graphics.Paint;
import android.util.AttributeSet;
import android.view.MotionEvent;
import android.widget.ImageView;
import android.widget.Toast;

public class DrawImage extends ImageView {

	Paint p;

	private float pointX, pointY;

	public DrawImage(Context context, AttributeSet attrs) {
		super(context, attrs);
		init();
	}

	private void init() {
		p = new Paint();
	}

	@Override
	protected void onDraw(Canvas canvas) {
		// TODO Auto-generated method stub
		// super.onDraw(canvas);
		super.onDraw(canvas);
		// if (getDrawable() == null) {
		// return; // couldn't resolve the URI
		// }
		// Bitmap b = ((BitmapDrawable) getDrawable()).getBitmap();
		// if (null == b) {
		// return;
		// }
		// canvas.drawBitmap(b, 0, 0, null);

		p.setColor(Color.parseColor("#ff2200"));
		canvas.drawCircle(200, 200, 50, p);
	}

	@Override
	public boolean onTouchEvent(MotionEvent event) {
		switch (event.getAction() & MotionEvent.ACTION_MASK) {
		case MotionEvent.ACTION_DOWN: {
			pointX = event.getX();
			pointY = event.getY();
			break;
		}
		case MotionEvent.ACTION_UP: {

			if (event.getX() == pointX && event.getY() == pointY) {
				click(event);
				performClick();
				return true;
			}
			break;
		}
		case MotionEvent.ACTION_MOVE: {

			break;
		}

		default:
			break;
		}
		return true;
	}
	
	@Override
	public boolean performClick() {
		// TODO Auto-generated method stub
		return super.performClick();
	}

	@Override
	protected void dispatchSetPressed(boolean pressed) {
		// TODO Auto-generated method stub
		super.dispatchSetPressed(pressed);
	}
	void click(MotionEvent e) {
		System.out.println( e.getX() + "," + e.getY());
//		Toast.makeText(getContext(), e.getX() + "," + e.getY(), Toast.LENGTH_SHORT).show();
	}
}