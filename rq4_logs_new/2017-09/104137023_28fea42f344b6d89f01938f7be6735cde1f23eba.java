package net.coolblossom.lycee.machinelearning.classification.kernels;

/**
 * Radial Basis Function Kernel（RBF Kernel / Gaussian Kernel）
 * 
 * @author ryouka0122@github
 *
 */
public class RBFKernel implements Kernel {

	/** パラメータ */
	private double sigma;

	/**
	 * コンストラクタ
	 * 
	 * @param sigma
	 */
	public RBFKernel(double sigma) {
		if (sigma <= 0.0) {
			throw new IllegalArgumentException("引数sigmaに０以下の値を指定することは出来ません．");
		}
		this.sigma = sigma;
	}

	@Override
	public double calc(double[] x1, double[] x2) {
		double v = 0.0;
		for (int i = 0; i < x1.length; i++) {
			v += x1[i] * x2[i];
		}
		return Math.exp(-sigma * v);
	}

}