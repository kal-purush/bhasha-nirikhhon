import java.io.BufferedReader;
import java.io.InputStreamReader;

import java.util.Comparator;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;

public class ClosestPair1 {

	private static Comparator<Point> CompareX, CompareY;

	public static void main(String[] args) throws Exception {
		BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
		int n;

			CompareX = new Comparator<Point>() {
			public int compare(Point p1, Point p2) { 
				float distance = p1.getX() - p2.getX();
				if (distance > 0)
					return 1;
				else if (distance < 0)
					return -1;
				else
					return 0;
			}
		};

			CompareY = new Comparator<Point>() {
			public int compare(Point p1, Point p2) { 
				float distance = p1.getY() - p2.getY();
				if (distance > 0)
					return 1;
				else if (distance < 0)
					return -1;
				else
					return 0;
			}
		};


		while ((n = Integer.parseInt(reader.readLine())) != 0)
		{
			Point[] points = new Point[n];
			for (int i = 0; i < n; i++)
			{
				String[] input = reader.readLine().split(" ");
				points[i] = new Point(Float.parseFloat(input[0]), 
					Float.parseFloat(input[1]));
			}

			Arrays.sort(points, CompareX);

			PointDistance solution = getClosestPairRecursive(points, 0, points.length);
			System.out.printf("%.2f %.2f %.2f %.2f \n", solution.getP1().getX(), solution.getP1().getY(),
												solution.getP2().getX(), solution.getP2().getY());
		}
	}

	private static PointDistance getClosestPairBruteForce(Point[] points, int lowerBound, int upperBound) {
		PointDistance minimumDistance, currentDistance;
		minimumDistance = new PointDistance();

		for (int i = lowerBound; i < upperBound - 1; i++)
			for (int j = i+1; j < upperBound; j++)
			{
				currentDistance = new PointDistance(points[j], points[i]);
				if (currentDistance.getDistance() < minimumDistance.getDistance())
					minimumDistance = currentDistance;
			}
		return minimumDistance;
	}

	private static PointDistance getClosestPairRecursive(Point[] points, int lowerBound, int upperBound) {
		int size = upperBound - lowerBound;
		if (size < 3)
			return getClosestPairBruteForce(points, lowerBound, upperBound);
		int midPoint = lowerBound + (size/2);
		
		PointDistance left = getClosestPairRecursive(points, lowerBound, midPoint);
		PointDistance right = getClosestPairRecursive(points, midPoint, upperBound);

		PointDistance minimumDistance = (left.getDistance() < right.getDistance()) ? left : right;
		Point median = points[midPoint];

		ArrayList<Point> closerPoints = new ArrayList<>();

		for (int i = lowerBound; i < upperBound; i++)
			if (Math.abs(points[i].getX() - median.getX()) < minimumDistance.getDistance())
				closerPoints.add(points[i]);

		Collections.sort(closerPoints, CompareY);

		PointDistance currentDistance = new PointDistance();

		for (int i = 0; i < closerPoints.size() - 1; i++)
			for (int j = i+1; j < closerPoints.size() 
				&& Math.abs(closerPoints.get(i).getY() - closerPoints.get(j).getY()) < minimumDistance.getDistance(); j++)
			{
				currentDistance = new PointDistance(closerPoints.get(i), closerPoints.get(j));
				if (currentDistance.getDistance() < minimumDistance.getDistance())
					minimumDistance = currentDistance;
			}

		return minimumDistance;
	}
}