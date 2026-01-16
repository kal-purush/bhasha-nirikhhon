/**
 * @author MingFang Li
 * Course: CSCI 340 - DATA STRUCTURES/ALGORITHM DSGN
 * Date: 04/02/2019
 * Assignment: 4
 * Project/Class Description:
 *
 * This program asks the user to input the name of the file which contains all the points
 * and then use divide and conquer algrithm to calculate and find the points that have the
 * closest distance and the distance between them.
 *
 * Known bugs: none
 */

// imports
import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Scanner;

// class ClosestPoints
public class ClosestPoints {
    // Array list X takes a set of points and sort them by increasing x value
    private ArrayList<Point> X;
    // Array list X takes the same set of points but sort them by increasing y value
    private ArrayList<Point> Y;

    // constructor instantiates two array lists
    public ClosestPoints() {
        X = new ArrayList<Point>();
        Y = new ArrayList<Point>();
    }

    /**
     * The main calls the method readPoints and calculate to read in the points
     * then find the closest points and calculate the distance, and
     * prints out how much time did it take to calculate.
     * @param args
     * @throws FileNotFoundException
     */
    public static void main(String[] args) throws FileNotFoundException {
        // class object
        ClosestPoints cp = new ClosestPoints();
        // call method readPoints
        cp.readPoints();
        // call method calculate
        // and see how much time it took to run the method
        long start = System.nanoTime();
        cp.calculate();
        long finish = System.nanoTime();
        long seconds = finish - start;
        // print out how much time it took to run the method
        System.out.println("It took " + (double) seconds / 1000000000.0 + " seconds to calculate.");
    }

    /**
     * This method asks the user to enter the name of the file which contains all the points,
     * then add all the points in two array lists X and Y, then sort the array lists by their x and y value,
     * then prints out how much time it took to read in all the points.
     * @throws FileNotFoundException
     */
    private void readPoints() throws FileNotFoundException {
        // heading
        System.out.println();
        System.out.println("Enter the file that has the points: ");
        // scanner to scan what users enter
        Scanner scanFile = new Scanner(System.in);
        // make the input as a file
        String pointsFile = scanFile.nextLine();
        long start = System.nanoTime();
        File file = new File(pointsFile);
        // scan the file
        Scanner scanPoints = new Scanner(file);
        System.out.println();
        System.out.println("Reading the file......");

        // split each line to two strings and type cast them to doubles as x and y values
        // and make each line a point then put the points into array lists.
        while (scanPoints.hasNextLine()) {
            String[] bothPoints = scanPoints.nextLine().split(" ");
            Point point = new Point(Double.parseDouble(bothPoints[0]),
                    Double.parseDouble(bothPoints[1]));

            X.add(point);
            Y.add(point);
        }
        // sort the array lists according to their x and y value
        // increasing order
        X.sort(Comparator.comparingDouble(x -> x.x));
        Y.sort(Comparator.comparingDouble(y -> y.y));
        // print out what file the program read in
        System.out.println("File read in: " + pointsFile);
        // close the scanners
        scanFile.close();
        scanPoints.close();
        // prints out how much time it took to read in the points
        long finish = System.nanoTime();
        long seconds = finish - start;
        System.out.println("It took " + (double) seconds / 1000000000.0 + " seconds to read in the points.");
    }

    /**
     * This method calls its helper method to use divide and conquer algorithm
     * to calculate and find the points that have the closest distance in the file.
     */
    private void calculate() {
        // call the helper method and print out which two points in the file have the
        // closest distance and print out the distance between them.
        System.out.println("Calculating......");
        Pair result = passin(X, Y);
        System.out.println("The closest points are: ");
        System.out.println("(" + result.p1.x + "," + result.p1.y + ")");
        System.out.println("(" + result.p2.x + "," + result.p2.y + ")");
        System.out.println("Distance = " + result.distance);
    }

    /**
     * This is the helper method of method calculate.
     * This method calculates which two points have the closest distance.
     * If there are less than 4 points in the X array, simply calculate the distance between
     * all the points and find the closest points.
     * else, split and conquer the X array and Y array recursively, then take only some of the
     * points we need to consider about and find the points that have the closest distance.
     * @param X is an array lists of points sorted by x value.
     * @param Y is the same array lists of points but sorted by y value.
     * @return a pair object that contains two closest points and the distance between them.
     */
    private Pair passin(ArrayList<Point> X, ArrayList<Point> Y) {
        // divide both X and Y array lists into two array lists left and right
        ArrayList<Point> XL = new ArrayList<>(X.subList(0, X.size() % 2 == 0 ? X.size() / 2 : (X.size() / 2) + 1));
        ArrayList<Point> XR = new ArrayList<>(X.subList((X.size() % 2 == 0 ? X.size() / 2 : (X.size() / 2) + 1), X.size()));
        ArrayList<Point> YL = new ArrayList<Point>();
        ArrayList<Point> YR = new ArrayList<Point>();

        // the smallest distance
        double dist;
        // if the size is less than 4
        // simply  calculate the distance between
        // all the points and find the closest points.
        if (X.size() < 4) {
            dist = distanceCalculate(X.get(0), X.get(1));
            Pair minPair = new Pair(X.get(0), X.get(1), dist);
            for (int i = 1; i < X.size(); i++) {
                // if (!(X.get(i).equals(X.get((i + 1) % X.size())))){
                dist = distanceCalculate(X.get(i), X.get((i + 1) % X.size()));
                if (dist < minPair.distance)
                    minPair = new Pair(X.get(i), X.get((i + 1) % X.size()), dist);
                //  }
                //  if (dist != 0) {
                //pairs.add(new Pair(X.get(i), X.get((i + 1) % X.size()), dist));
                //   } else {
                //       System.out.println("Error: Distance is 0.");
                //   }
            }
            return minPair;
        } else {
            // fill the left and right array lists from array list Y
            for (Point p : Y) {
                if (p.x < XR.get(0).x) {
                    YL.add(p);
                } else {
                    YR.add(p);
                }
            }

            // recursively conquer the array lists
            Pair pair1 = passin(XL, YL);
            Pair pair2 = passin(XR, YR);

            // pair object
            Pair shortDis;
            // find which pair has the smaller distance
            if (pair1.distance < pair2.distance) {
                shortDis = pair1;
            } else {
                shortDis = pair2;
            }

            // find the middle line L
            double mid = (XL.get(XL.size() - 1).x + XR.get(0).x) / 2;
            // find the limit of both left and right side
            double line = mid - shortDis.distance;
            double line2 = mid + shortDis.distance;

            // array list yPrime only takes the points that we need to
            // consider about
            ArrayList<Point> yPrime = new ArrayList<Point>();

            // fill the array list yPrime
            for (Point p : Y) {
                if (p.x < line2 && p.x > line) {
                    yPrime.add(p);
                }
            }

            // find the points that have closest distance in array list yPrime
            for (int i = 0; i < yPrime.size(); i++) {
                for (int j = 1; j < 5 && i + j < yPrime.size(); j++) {
                    double dis = distanceCalculate(yPrime.get(i), yPrime.get(i + j));
                    if (dis < shortDis.distance) {
                        shortDis = new Pair(yPrime.get(i), yPrime.get(i + j), dis);
                    }
                }
            }

            // return the pair of points that have the closest distance
            // and the distance between them
            return shortDis;
        }
    }

    // the formula of calculating the distance of two points
    private double distanceCalculate(Point p1, Point p2) {
        return Math.sqrt(Math.pow(p1.x - p2.x, 2) + Math.pow(p1.y - p2.y, 2));
    }

    // pair class contains two points and the distance between them
    private class Pair {
        private Point p1;
        private Point p2;
        private double distance;

        private Pair(Point p1, Point p2, double distance) {
            this.p1 = p1;
            this.p2 = p2;
            this.distance = distance;
        }
    }

    // point class contains a x and a y value and two methods to get x value
    // and y value
    public class Point {
        private double x;
        private double y;

        private Point(double x, double y) {
            this.x = x;
            this.y = y;
        }

        public double getX() {
            return x;
        }
        public double getY() {
            return y;
        }
    }
}