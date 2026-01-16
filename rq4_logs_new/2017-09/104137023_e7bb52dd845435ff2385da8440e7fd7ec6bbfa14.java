package net.coolblossom.lycee.utils;

public class VectorUtil {

	public static double[] multiply(double[] v, double scalar) {
		double[] result = v.clone();
		for(int i=0 ; i<v.length ; i++) {
			result[i] *= scalar;
		}
		return result;
	}

	public static double[] multiplyS(double[] v, double scalar) {
		for(int i=0 ; i<v.length ; i++) {
			v[i] *= scalar;
		}
		return v;
	}

}