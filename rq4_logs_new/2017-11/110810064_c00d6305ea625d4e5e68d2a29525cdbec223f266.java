/**Note:
 * Algorithm itself is straight forward. For 2 lines l1 and l2, if at some x1 we have l1y1 > l2y1, then if at some x2 > x1 we have
 * l1y2 <= l2y2, these 2 lines intersect at range [x1,x2]. Just need to be careful on conditional statements.
 * 
 * Instruction:
 *     final Line l1 = new Line(-3, 1, 1, 5);
 *     final Line l2 = new Line(-1, 5, 4, 2);
 *     System.out.println(LineIntersection.detect(l2, l1));
 */
package com.makebono.algorithms.computationalgeometry.lineintersectiondetection;

import java.math.BigDecimal;

import com.makebono.algorithms.computationalgeometry.lineintersectiondetection.tools.Line;

/** 
 * @ClassName: LineIntesection 
 * @Description: For detect if there's intersection in 2 lines. Let's say both 2 lines are not verticle here.
 * @author makebono
 * @date 2017年11月23日 下午1:32:01 
 *  
 */
public class LineIntersection {
    public static boolean detect(final Line l1, final Line l2) {
        if (((l1.maxX().equals(l2.maxX())) && (l1.maxXY().equals(l2.maxXY())))
                || ((l1.minX().equals(l2.minX()) && (l1.minXY().equals(l2.minXY()))))
                || ((l1.maxX().equals(l2.minX()) && (l1.maxXY().equals(l2.minXY()))))
                || ((l1.minX().equals(l2.maxX()) && (l1.minXY().equals(l2.maxXY()))))) {
            return true;
        } else {
            if ((l1.minX().compareTo(l2.minX()) < 0) && (l1.maxX().compareTo(l2.minX()) < 0)) {
                return false;
            } else if ((l2.minX().compareTo(l1.minX()) < 0) && (l2.maxX().compareTo(l1.minX()) < 0)) {
                return false;
            } else {
                // l1 is on the left side of l2.(Talking about l1X1 < l2X1
                if (l1.minX().compareTo(l2.minX()) <= 0) {
                    final BigDecimal tempY11 = l1.YAt(l2.minX());

                    // tempY11 < y21
                    if (tempY11.compareTo(l2.minXY()) < 0) {
                        if (l1.maxX().compareTo(l2.maxX()) <= 0) {
                            final BigDecimal tempY22 = l2.YAt(l1.maxX());
                            return l1.maxXY().compareTo(tempY22) >= 0 ? true : false;
                        } else {
                            final BigDecimal tempY12 = l1.YAt(l2.maxX());
                            return tempY12.compareTo(l2.maxXY()) >= 0 ? true : false;
                        }

                        // tempY11 > y21
                    } else if (tempY11.compareTo(l2.minXY()) > 0) {
                        if (l1.maxX().compareTo(l2.maxX()) <= 0) {
                            final BigDecimal tempY22 = l2.YAt(l1.maxX());
                            return l1.maxXY().compareTo(tempY22) <= 0 ? true : false;
                        } else {
                            final BigDecimal tempY12 = l1.YAt(l2.maxX());
                            return tempY12.compareTo(l2.maxXY()) <= 0 ? true : false;
                        }
                    } else {
                        // Intersect at l2.minX().
                        return true;
                    }
                }
                // l2 is on the left side of l1.
                else {
                    final BigDecimal tempY21 = l2.YAt(l1.minX());

                    if (tempY21.compareTo(l1.minXY()) < 0) {
                        if (l2.maxX().compareTo(l1.maxX()) <= 0) {
                            final BigDecimal tempY12 = l1.YAt(l2.maxX());
                            return l2.maxXY().compareTo(tempY12) >= 0 ? true : false;
                        } else {
                            final BigDecimal tempY22 = l2.YAt(l1.maxX());
                            return tempY22.compareTo(l1.maxXY()) >= 0 ? true : false;
                        }
                    } else if (tempY21.compareTo(l2.minXY()) > 0) {
                        if (l2.maxX().compareTo(l1.maxX()) <= 0) {
                            final BigDecimal tempY12 = l1.YAt(l2.maxX());
                            return l2.maxXY().compareTo(tempY12) <= 0 ? true : false;
                        } else {
                            final BigDecimal tempY22 = l2.YAt(l1.maxX());
                            return tempY22.compareTo(l1.maxXY()) <= 0 ? true : false;
                        }
                    } else {
                        // Intersect at l1.minX().
                        return true;
                    }
                }
            }
        }
    }
}