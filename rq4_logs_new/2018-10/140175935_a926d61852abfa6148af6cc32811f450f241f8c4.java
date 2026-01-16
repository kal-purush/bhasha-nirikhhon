package com.maxapp.dansu.astraplayer;

import android.animation.AnimatorSet;
import android.animation.ObjectAnimator;
import android.support.constraint.ConstraintLayout;
import android.view.MotionEvent;
import android.view.animation.Animation;
import android.view.animation.Transformation;
import android.widget.ImageView;

import static java.lang.Math.sqrt;

public class ImageAnimator {
    private int startx;
    private int starty;
    private int imgDefaultX;
    private int imgDefaultY;
    public ImageAnimator(int defX, int defY){
        imgDefaultX = defX;
        imgDefaultY = defY;
    }

    public void animate(MotionEvent event, final ImageView imgView){
        switch (event.getAction()) {
            case MotionEvent.ACTION_DOWN:
                startx = (int)event.getRawX();
                starty = (int)event.getRawY();
                ConstraintLayout.LayoutParams tmpParams = (ConstraintLayout.LayoutParams) imgView.getLayoutParams();
                tmpParams.topMargin = imgDefaultY;
                tmpParams.leftMargin = imgDefaultX;
                imgView.setLayoutParams(tmpParams);



            case MotionEvent.ACTION_MOVE:
                ConstraintLayout.LayoutParams mParams = (ConstraintLayout.LayoutParams) imgView.getLayoutParams();
                int x = (int) event.getRawX();
                int y = (int) event.getRawY();
                mParams.leftMargin = imgDefaultX - (startx - x);
                mParams.topMargin = imgDefaultY - (starty - y);
                imgView.setLayoutParams(mParams);
                break;

            case MotionEvent.ACTION_UP:

                ConstraintLayout.LayoutParams params = (ConstraintLayout.LayoutParams) imgView.getLayoutParams();
                final int deltaY = imgDefaultY - params.topMargin;
                final int topMargin = params.topMargin;
                final int deltaX = imgDefaultX - params.leftMargin;
                final int leftMargin = params.leftMargin;

                Animation a = new Animation() {

                    @Override
                    protected void applyTransformation(float interpolatedTime, Transformation t) {
                        ConstraintLayout.LayoutParams params = (ConstraintLayout.LayoutParams) imgView.getLayoutParams();
                            params.topMargin = (int)(topMargin + (deltaY * interpolatedTime));
                                params.leftMargin = (int)(leftMargin + (deltaX * interpolatedTime));
                        imgView.setLayoutParams(params);
                    }
                };
                a.setDuration(500); // in ms
                imgView.startAnimation(a);


                break;
            default:
                break;
        }
    }
}