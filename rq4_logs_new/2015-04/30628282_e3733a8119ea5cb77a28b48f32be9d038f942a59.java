/**
 * 
 */
package edu.pitt.hashtable;

/**
 * Max Points on a Line  $149 --- Accepted 312ms
 * Given n points on a 2D plane, find the maximum number of points that lie on the same straight line.
 * @author yanma
 * @since 2015-04-29
 * @version 2015-04-29
 */
public class MaxPointsonaLine_149 {

	public static int maxPoints(Point[] points) {
		if (points.length < 3) {
            return points.length;
        }
        int max = 0;
        
        for (int i=0; i < points.length - 2; i++) {
            boolean[] checked = new boolean[points.length];
            for (int j = i + 1; j < points.length; j++) {
                if (checked[j]) {continue;}
                while (j < points.length && points[j].x == points[i].x && points[j].y == points[i].y) {
                	j++;
                }
                int count = (j == points.length) ? 1: 2;
                for (int t = i + 1; t < j; t++) {
                	if (points[t].x == points[i].x && points[t].y == points[i].y) {
                    	count++;
                    }
                }
                
                for (int k = j + 1; k < points.length; k++) {
                    if (isOneLine(points[i], points[j], points[k])) {
                        count++;
                        checked[k] = true;
                    }
                }
                
                if (count > max) {
                    max = count;
                }
            }
        }
        
        return max;
	}
	
	public static boolean isOneLine(Point a, Point b, Point c) {
        return (b.y - a.y) * (c.x - a.x) == (b.x - a.x) * (c.y - a.y);
    }
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
//		[0,9],[138,429],[115,359],[115,359],[-30,-102],[230,709],[-150,-686],[-135,-613],[-60,-248],[-161,-481],[207,639],[23,79],[-230,-691],[-115,-341],[92,289],[60,336],[-105,-467],[135,701],[-90,-394],[-184,-551],[150,774]
		Point p1 = new Point(0,-12);
		Point p2 = new Point(5,2);
		Point p3 = new Point(2,5);
		Point p4 = new Point(0,-5);
		Point p5 = new Point(1,5);
		Point p6 = new Point(2,-2);
		Point p7 = new Point(5,-4);
		Point p8 = new Point(3,4);
		Point p9 = new Point(-2,4);
		Point p10 = new Point(-1,4);
		Point p11 = new Point(0,-5);
		Point p12 = new Point(0,-8);
		Point p13 = new Point(-2,-1);
		Point p14 = new Point(0,-11);
		Point p15 = new Point(0,-9);
		
		Point[] points = new Point[15];
		points[0] = p1;
		points[1] = p2;
		points[2] = p3;
		points[3] = p4;
		points[4] = p5;
		points[5] = p6;
		points[6] = p7;
		points[7] = p8;
		points[8] = p9;
		points[9] = p10;
		points[10] = p11;
		points[11] = p12;
		points[12] = p13;
		points[13] = p14;
		points[14] = p15;
		
		System.out.println(maxPoints(points));
		
		
	}

}