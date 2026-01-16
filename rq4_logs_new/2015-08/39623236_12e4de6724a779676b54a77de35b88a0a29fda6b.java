package ch.mpluess.mandelbrotexplorer;

public class MandelbrotState {
	public final double minX;
	public final double maxX;
	public final double minY;
	public final double maxY;
	public final double stepX;
	public final double stepY;
	
	public MandelbrotState(double minX, double maxX, double minY,
			double maxY, double stepX, double stepY) {
		this.minX = minX;
		this.maxX = maxX;
		this.minY = minY;
		this.maxY = maxY;
		this.stepX = stepX;
		this.stepY = stepY;
	}
}