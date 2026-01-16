package com.example.luka.openglestest.engine;

import com.example.luka.openglestest.MojRenderer;

import static android.opengl.Matrix.multiplyMM;
import static android.opengl.Matrix.multiplyMV;

/**
 * Created by Luka on 2.8.2016..
 */
public class Controls
{
    static float dRoll, dPitch;
    static float rollSensitivity = 30;
    static float pitchSensitivity = 30;
    static float alpha = 0.05f, rollAlpha, pitchAlpha;

    static public float[] acceleration = new float[3], accelerationTm1 = new float[3], acceleratonInit = new float[3];
    static public boolean accelerationInitBool = true;
    static float[] tempMatrix = new float[16];
    static float[] tempVector = new float[4];

    public static final Object mutex = new Object();

    static GLObject controlledObject;

    public static void SetControlledObject( GLObject object )
    {
        controlledObject = object;
    }

    public static void SetOrientationFromAcceleration( float[] acceleration )
    {

        if ( accelerationInitBool )
        {
            acceleratonInit = acceleration.clone();
            accelerationInitBool = false;
        }

        if ( MojRenderer.plane == null )
            return;

        acceleration[0] -= acceleratonInit[0];
        acceleration[1] -= acceleratonInit[1];

        acceleration = ExponentialSmoothing( acceleration, accelerationTm1, alpha );

        dRoll = -acceleration[1] * rollSensitivity;
        dPitch = -acceleration[0] * pitchSensitivity;

        android.opengl.Matrix.setIdentityM( tempMatrix, 0 );

        synchronized(mutex)
        {
            android.opengl.Matrix.setIdentityM(controlledObject.rotationMatrix, 0);

            android.opengl.Matrix.setRotateM(
                    tempMatrix, 0, dRoll, controlledObject.yAxis[0], controlledObject.yAxis[1], controlledObject.yAxis[2]);
            multiplyMM(controlledObject.rotationMatrix, 0, tempMatrix, 0, controlledObject.rotationMatrix, 0);

            multiplyMV(tempVector, 0, tempMatrix, 0, controlledObject.xAxis, 0);
            //android.opengl.Matrix.setRotateM(tempMatrix, 0, dPitch, 1.0f, 0, 0);
            android.opengl.Matrix.setRotateM(tempMatrix, 0, dPitch, tempVector[0], tempVector[1], tempVector[2]);
            multiplyMM(controlledObject.rotationMatrix, 0, tempMatrix, 0, controlledObject.rotationMatrix, 0);

            tempVector = controlledObject.initOrientation.clone();
            multiplyMV(controlledObject.orientation, 0, controlledObject.rotationMatrix, 0, tempVector, 0);
        }

        accelerationTm1 = acceleration;

    }

    private static float[] ExponentialSmoothing( float[] xt, float[] stm1, float alpha )
    {
        if ( stm1 == null )
            return xt;
        for ( int i=0; i<xt.length; i++ ) {
            //stm1[i] = stm1[i] + alpha * (xt[i] - stm1[i]);
            stm1[i] = alpha * xt[i] + (1 - alpha) * stm1[i];
        }
        return stm1;
    }

}