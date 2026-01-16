/*************************************************************************
 * Name:
 * Email:
 *
 * Compilation:  javac Point.java
 * Execution:
 * Dependencies: StdDraw.java
 *
 * Description: An immutable data type for points in the plane.
 *
 *************************************************************************/

public class Fast {
   public static void main(String[] args) {
	   In in = new In(args[0]);      // input file
       int N = in.readInt();         // N points

       if (N > 4)
       {
    	   int Count = 0;
	       Point [] point = new Point[N];
	       for (int i = 0; i < N; i++) {
	            int x = in.readInt();
	            int y = in.readInt();
	            point[Count++] = new Point(x, y);
	        }
       }
   }
}