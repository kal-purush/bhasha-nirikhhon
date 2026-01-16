package counters;

public class SimpleCounter {
	private long start;
	private long end;
	private int c;

	public void begin() {
		start = System.nanoTime();
	}

	public void end() {
		end = System.nanoTime();
	}

	public void tick() {
		c++;
	}

	public void print() {
		double delta = (end - start) / 1e6;
		System.out.printf("time: %.2f ms; ticks: %s; ticks per second %.2f\n", delta, c, 1e3 * c / delta);
	}
}