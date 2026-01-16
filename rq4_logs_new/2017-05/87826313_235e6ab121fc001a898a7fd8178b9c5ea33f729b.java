package dominio;

public class MyRandomStub extends RandomGenerator {

	private double h_double;
	public final static double HDOUBLE = 0.49; 
	
	public MyRandomStub(final double h_double) {
		this.h_double = h_double;
	}
	
	@Override
	public double nextDouble() {
		return this.h_double;
	}

	@Override
	public int nextInt(int max) {
		return max-1;
	}

}