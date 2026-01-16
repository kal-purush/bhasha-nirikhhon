package com.xiong.bessel.path;

import android.animation.TypeEvaluator;
import android.graphics.PointF;

/**
 * @author: xiong
 * @time: 2018/11/23
 * @说明:
 */
public class BezierEvaluator implements TypeEvaluator<PointF> {

    private PointF mFlagPoint;

    public BezierEvaluator(PointF flagPoint) {
        mFlagPoint = flagPoint;
    }

    @Override
    public PointF evaluate(float v, PointF pointF, PointF t1) {
        return BezierUtil.CalculateBezierPointForQuadratic(v, pointF, mFlagPoint, t1);
    }
}