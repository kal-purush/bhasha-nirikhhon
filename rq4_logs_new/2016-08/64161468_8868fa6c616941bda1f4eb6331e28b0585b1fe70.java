package com.example.luka.openglestest.engine;

import static android.opengl.Matrix.setLookAtM;
import static com.example.luka.openglestest.engine.GLCommon.SetCenter;
import static com.example.luka.openglestest.engine.GLCommon.SetEyePosition;
import static com.example.luka.openglestest.engine.GLCommon.center;
import static com.example.luka.openglestest.engine.GLCommon.eyePosition;
import static com.example.luka.openglestest.engine.GLCommon.eyePositionTm1;
import static com.example.luka.openglestest.engine.GLCommon.up;
import static com.example.luka.openglestest.engine.GLCommon.viewMatrix;

/**
 * Created by Luka on 6.8.2016..
 */
public abstract class GLCamera
{
    GLObject trackedObject;

    float[] pipeOrientation = new float[4];

    public GLCamera() {};

    public abstract void UpdateCamera();

}
