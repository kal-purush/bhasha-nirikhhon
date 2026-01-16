import edu.princeton.cs.algs4.StdIn;
import edu.princeton.cs.algs4.StdOut;


public class Subset {
	
	public static void main (String[] args) {
		int numberOfTimes = Integer.parseInt(args[0]);
		RandomizedQueue<String> queue = new RandomizedQueue<String>();
		while (StdIn.hasNextLine() && !StdIn.isEmpty()) {
			queue.enqueue(StdIn.readString());
		}
		for (int i = 0; i < numberOfTimes; i++) {
			StdOut.println(queue.iterator().next());
		}
	}
}