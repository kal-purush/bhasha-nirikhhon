package com.makebono.algorithms.tools;

/** 
 * @ClassName: FastInverseSqrt 
 * @Description: Everyone loves id
 * @author  0x5f3759df
 * @date 2017年11月29日 上午11:56:37 
 *  
 */
public class FastInverseSqrt {
    public static float sqrt(final float number) {
        int i;
        float x2, y;
        final float threehalfs = 1.5F;

        x2 = number * 0.5F;
        y = number;
        i = Float.floatToIntBits(y); // evil floating point bit level hacking
        i = 0x5f3759df - (i >> 1); // what the fuck?
        y = Float.intBitsToFloat(i);
        y = y * (threehalfs - (x2 * y * y)); // 1st iteration
        // y = y * (threehalfs - (x2 * y * y)); // 2nd iteration, this can be removed

        return 1 / y; // y = 1/sqrt(number)
    }
}