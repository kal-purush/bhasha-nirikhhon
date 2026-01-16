package algorithms.bst;

import java.awt.*;
import java.util.HashSet;

/**
 * Bruno Cardoso on 26/02/2015.
 *
 * Given an array of Points in the Cartesian system,
 * return the list of points of the line that passes through the most points.
 *
 * Time complexity: O(n^2)
 * Space complexity: O(
 */
public class CollinearPoints {

    public static HashSet<Point> count(Point[] points){
        HashSet<Point> maxResult = new HashSet<>();

        for (int i = 0; i < points.length-1; i++) {
            Point first = points[i];
            Point second = points[i+1];

            HashSet<Point> result = new HashSet<>();
            result.add(first);
            result.add(second);
            for (int j = i+2; j < points.length; j++) {
                Point third = points[j];
                if(isCollinear(first, second, third)){
                    result.add(third);
                }
            }

            // update max
            if(maxResult.size() < result.size()){
                maxResult = result;
            }
        }
        return maxResult;
    }

    private static boolean isCollinear(Point first, Point second, Point third) {
        return 0 == (first.getX()*(second.getY() - third.getY())
                        + second.getX()*(third.getY() - first.getY())
                        + third.getX()*(first.getY() - second.getY()));
    }
}