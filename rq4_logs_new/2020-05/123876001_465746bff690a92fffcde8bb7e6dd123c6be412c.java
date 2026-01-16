package practice.codeforces;

import java.util.Scanner;

/**
 * @Link https://codeforces.com/problemset/problem/1/C
 * status : ac
 * 思路 对于给定的三角形，其外接圆半径大小是一定的；在外接圆半径大小一定的情况下，要使正多边形面积最小，就是使正多边形的边数最少，
 * 也就是使得每条边所对应的圆心角最大。而最大圆心角 = 三角形三条边所对应的三个圆心角的公约数
 *
 * 问题： 目前需要保证c边为最大边
 */
public class C_Ancient_Berland_Circus {
    private static double PI = 3.1415926;
    private static double MIN_ANGLE = 60;
    private static double ZERO = 1e-6;
    public static void main(String[] args) {
        double x1=0.0000, y1=0.0000;
        double x2=0.0000, y2=1.0000;
        double x3=1.0000, y3=1.0000;
        Scanner sc = new Scanner(System.in);
        x1 = sc.nextDouble();
        y1 = sc.nextDouble();
        x2 = sc.nextDouble();
        y2 = sc.nextDouble();
        x3 = sc.nextDouble();
        y3 = sc.nextDouble();
        DoubleC[] coordinates = new DoubleC[3];
        coordinates[0] = new DoubleC();
        coordinates[1] = new DoubleC();
        coordinates[2] = new DoubleC();
        coordinates[0].x = x1;coordinates[0].y=y1;
        coordinates[1].x = x2;coordinates[1].y=y2;
        coordinates[2].x = x3;coordinates[2].y=y3;
        System.out.println(String.format("%.6f", processing2(coordinates)));
    }

    private static double getDistance(DoubleC a, DoubleC b) {
        return Math.sqrt((a.x - b.x)*(a.x - b.x) + (a.y - b.y)*(a.y-b.y));
    }

    private static double processing2(DoubleC[] coordinates) {
//        double a = Math.sqrt((coordinates[1].x-coordinates[0].x)*(coordinates[1].x-coordinates[0].x) +
//                             (coordinates[1].y-coordinates[0].y)*(coordinates[1].y-coordinates[0].y));
//        double b = Math.sqrt((coordinates[2].x-coordinates[1].x)*(coordinates[2].x-coordinates[1].x) +
//                             (coordinates[2].y-coordinates[1].y)*(coordinates[2].y-coordinates[1].y));
//        double c = Math.sqrt((coordinates[0].x-coordinates[2].x)*(coordinates[0].x-coordinates[2].x) +
//                             (coordinates[0].y-coordinates[2].y)*(coordinates[0].y-coordinates[2].y));
        double a = getDistance(coordinates[0], coordinates[1]);
        double b = getDistance(coordinates[0], coordinates[2]);
        double c = getDistance(coordinates[1], coordinates[2]);
        if (a > b && a > c) {
            c = c + a;
            a = c - a;
            c = c - a;
        }
        if (b > a && b > c) {
            c = b + c;
            b = c - b;
            c = c - b;
        }
        double p = (a + b + c) / 2.0;
        double S = Math.sqrt(p * (p - a) * (p - b) * (p - c));
        double R = a * b * c / (4.0 * S);

        double A = arcCos(a, R, R);
        double B = arcCos(b, R, R);
        double C = 2.0 * PI - A - B;
        double angle = fgcd(A, fgcd(B, C));
        double area = 0.5 * R * R * Math.sin(angle) * (2.0 * PI /angle);
        return area;
    }

    private static double arcCos(double a, double b, double c) {
        return Math.acos((b*b + c*c - a*a)/ (2.0 * b * c));
    }

    private static boolean eqzero(double a, double b) {
        return (a - b) < 1e-3;
    }

    private static double fmod(double a, double b) {
        int i = (int)(a / b);
//        System.out.println(a - b*i);
        if ((a - b * i) < 1e-3) {
            return 0;
        } else {
            return a - b * i;
        }

    }
    private static double fgcd(double a, double b) {
        if (eqzero(a, 0))
            return b;
        if (eqzero(b, 0))
            return a;
//        return fgcd(b, a % b);
        return fgcd(b, fmod(a, b));
    }

    static class DoubleC {
        public double x;
        public double y;
    }



    /*
     * deprecated
     * 针对正多边形 面积公式
     * 公式 ： P = n * s (P 周长， n 边数， s边长)
     *        a = s / (2 * tan(180/n)) (a 边心距)
     *        A = P * a / 2     (A 面积)
     *
     *        弧度角度换算  angle = hudu * 180 / PI
     * @return
     */
    private static double processing(DoubleC[] loc) {
        double[] cx = new double[3];
//        double cx1 = getAngle(x2 - x1, y2 - y1, x3 - x1, y3 - y1);
//        double cx2 = getAngle(x1 - x2, y1 - y2, x3 - x2, y3 - y2);
//        double cx3 = getAngle(x1 - x3, y1 - y3, x2 - x3, y2 - y3);
        for (int i = 0; i< loc.length; i++) {
            cx[i] = getAngle(loc[(i+1)%3].x - loc[i].x, loc[(i+1)%3].y - loc[i].y,
                             loc[(i+2)%3].x - loc[i].x, loc[(i+2)%3].y - loc[i].y);
        }

        int maxAngleLoc = -1;
        double maxAngle = 0;
        for (int i=0; i<cx.length; i++) {
            if (maxAngle < cx[i]) {
                maxAngle = cx[i];
                maxAngleLoc = i;
            }
        }
        maxAngleLoc = 1;
        maxAngle = cx[1];
        double s = Math.sqrt((loc[(maxAngleLoc+1) % 3].x - loc[maxAngleLoc].x) * (loc[(maxAngleLoc+1) % 3].x - loc[maxAngleLoc].x)
                            + (loc[(maxAngleLoc+1) % 3].y - loc[maxAngleLoc].y) * (loc[(maxAngleLoc+1) % 3].y - loc[maxAngleLoc].y));
        double finalAngle = maxAngle;

        double n = 360 / (180 - finalAngle);

        double P = n * s;
        double a = s / (2 * Math.tan((180 / n) * PI / 180));
        double A = P * a / 2;
        return A;
    }

    private static double getAngle(double x1, double y1, double x2, double y2) {
        double a = x1 * x2 + y1 * y2;
        double b = Math.sqrt(x1 * x1 + y1 * y1);
        double c = Math.sqrt(x2 * x2 + y2 * y2);
        double g = a / (b * c);
        double result = Math.acos(g);
//        System.out.println(String.format("%.6f", result * 180 / PI));
        return result * 180 / PI ;
    }
}