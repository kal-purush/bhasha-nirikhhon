package com.project.nicki.displaystabilizer.dataprocessor.utils;

import android.renderscript.Matrix3f;
import android.renderscript.Matrix4f;
import  java.lang.Math;
/**
 * Created by nicki on 7/3/2016.
 */
public class Matrix2Quaternion {
    public static float[] setQuatFromMatrix(Matrix3f mat)
    {
        double x,y,z,w;
        double az, ay, ax;
        double ai, aj, ak;
        double si, sj, sk;
        double ci, cj, ck;
        double cy, cc, cs, sc, ss;

        cy = Math.sqrt( mat.get(0,0) * mat.get(0,0) + mat.get(1,0) * mat.get(1,0) );

        if( cy > Math.ulp(1) )
        {
            ax = Math.atan2( mat.get(2,1), mat.get(2,2) );
            ay = Math.atan2( -mat.get(2,0), cy );
            az = Math.atan2( mat.get(1,0), mat.get(0,0) );
        }
        else
        {
            ax = Math.atan2( -mat.get(1,2), mat.get(1,1) );
            ay = Math.atan2( -mat.get(2,0), cy );
            az = 0.0;
        }

        ai = ax / 2.0;
        aj = ay / 2.0;
        ak = az / 2.0;

        ci = Math.cos( ai );
        si = Math.sin( ai );
        cj = Math.cos( aj );
        sj = Math.sin( aj );
        ck = Math.cos( ak );
        sk = Math.sin( ak );
        cc = ci * ck;
        cs = ci * sk;
        sc = si * ck;
        ss = si * sk;

        x = cj * sc - sj * cs;
        y = cj * ss + sj * cc;
        z = cj * cs - sj * sc;
        w = cj * cc + sj * ss;
        return new float[]{(float) w, (float) x, (float) y, (float) z};
    }
}