package com.company;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by unsee on 2016/6/13.
 */
public class MaxPoints {
    static class Point {
        int x;
        int y;
        Point() { x = 0; y = 0; }
        Point(int a, int b) { x = a; y = b; }
    }

    public class Line {
        int a;
        int b;
        int c;

        public Line(int i1, int i2, int i3) {
            a = i1;
            b = i2;
            c = i3;
        }

        @Override
        public boolean equals(Object o) {
            Line l = (Line) o;
            return l.a == a && l.b == b && l.c == c;
        }

        @Override
        public int hashCode() {
            return 1791 * a + 143 * b + 7 * c;
        }
    }


    public int maxPoints(Point[] points) {
        if (points == null || points.length == 0) {
            return 0;
        }
        int max = 1;
        for (int i = 0; i < points.length; i++) {
            int same = 1;
            Map<Line, Integer> map = new HashMap<>();
            for (int j = i + 1; j < points.length; j++) {
                Point p1 = points[i];
                Point p2 = points[j];
                int x1 = p1.x;
                int x2 = p2.x;
                int y1 = p1.y;
                int y2 = p2.y;
                int a = y2 - y1;
                int b = x1 - x2;
                if (a == 0 && b == 0) {
                    same++;
                    continue;
                }
                int c = x2 * y1 - y2 * x1;
                int gcd = gcd(a, b);
                gcd = gcd(gcd, c);
                a /= gcd;
                if (a < 0) {
                    a *= -1;
                    gcd *= -1;
                }
                b /= gcd;
                if (a == 0 && b < 0) {
                    b *= -1;
                    gcd *= -1;
                }
                c /= gcd;
                Line line = new Line(a, b, c);
                int count = 2;
                if (map.containsKey(line)) {
                    count = map.get(line) + 1;
                }
                map.put(line, count);
                max = Math.max(max, count);
            }
            max = Math.max(max, same);
        }

        return max;
    }

    private static int gcd(int a, int b) {
        if (a == 0 || b == 0) {
            return a + b;
        }
        return gcd(b,a % b);
    }

    public static void main(String args[]) {
        Point[] points = new Point[3];
        points[0] = new Point(-1, -1);
        points[1] = new Point(-2, -2);
        points[2] = new Point(2, 2);
        MaxPoints m = new MaxPoints();
        m.maxPoints(points);
    }
}