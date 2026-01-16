package com.cg.quickcircle;

import android.graphics.Camera;  
import android.graphics.Matrix;  
import android.view.animation.Animation;  
import android.view.animation.Transformation;  

public class CGAnimation extends Animation {  
    public static final boolean ROTATE_DECREASE = true;  
    public static final boolean ROTATE_INCREASE = false;  
    public static final float DEPTH_Z = 310.0f;  
    public static final long DURATION = 800l;  
    private final boolean type;  
    private final float centerX;  
    private final float centerY;  
    private Camera camera;  
  
    public CGAnimation(float cX, float cY, boolean type) {  
        centerX = cX;  
        centerY = cY;  
        this.type = type;  
        setDuration(DURATION);  
    }  
  
    public void initialize(int width, int height, int parentWidth, int parentHeight) {  
        super.initialize(width, height, parentWidth, parentHeight);  
        camera = new Camera();  
    }  
  
    protected void applyTransformation(float interpolatedTime, Transformation transformation) {  
        float degree = 0;
        if (type == ROTATE_DECREASE) {  
            //90 -> 0
        	degree = -(90 - 90 * interpolatedTime);
        } else if (type == ROTATE_INCREASE) {  
        	//90 -> 180
        	degree = 90 - 90 * interpolatedTime;
        }
        
        final Matrix matrix = transformation.getMatrix();  
        camera.save();  
        camera.rotateY(degree);  
        camera.getMatrix(matrix);  
        camera.restore();  
        matrix.preTranslate(-centerX, -centerY);  
        matrix.postTranslate(centerX, centerY);  
    }
  
}