package com.example.dongja94.sampleanimation;

import android.graphics.Camera;
import android.graphics.Matrix;
import android.view.animation.Animation;
import android.view.animation.Transformation;

/**
 * Created by dongja94 on 2016-01-25.
 */
public class My3DAnimation extends Animation {
    Camera mCamera;
    public My3DAnimation() {
        mCamera = new Camera();
    }

    int centerX, centerY;

    @Override
    public void initialize(int width, int height, int parentWidth, int parentHeight) {
        super.initialize(width, height, parentWidth, parentHeight);
        centerX = width / 2;
        centerY = height / 2;
    }

    @Override
    protected void applyTransformation(float interpolatedTime, Transformation t) {
        mCamera.save();

        mCamera.rotateX(interpolatedTime * 45);


        Matrix m = t.getMatrix();

        mCamera.getMatrix(m);

        mCamera.restore();

        m.preTranslate(-centerX, -centerY);
        m.postTranslate(centerX, centerY);

    }
}