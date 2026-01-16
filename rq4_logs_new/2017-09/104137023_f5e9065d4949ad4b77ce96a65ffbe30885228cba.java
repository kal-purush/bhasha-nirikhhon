package net.coolblossom.lycee.arithmetics.functions;

import java.util.stream.Stream;

/**
 * ファンクタの基底クラス
 * @author ryouka0122@github
 *
 */
public abstract class Functor implements Calculator {


	/** 次数 */
	protected int dimension;

	/** 各次数のパラメータ */
	protected double[] parameters;

	/**
	 * Constructor
	 * @param dimension
	 */
	public Functor(int dimension) {
		if(dimension < 1) {
			throw new IllegalArgumentException("dimensionは1以上でないといけません[dimension="+dimension+"]");
		}
		this.dimension = dimension;
		this.parameters = Stream.iterate(0.0, t->t).limit(1+dimension).mapToDouble(t->t).toArray();
	}

	/**
	 * 次数の取得
	 * @return
	 */
	public int getDimension() {
		return this.dimension;
	}

	/**
	 * パラメータの設定
	 * @param parameters 新規パラメータ
	 * @return 更新前パラメータ
	 */
	public double[] setParameters(double[] parameters) {
		double[] before = this.parameters;
		this.parameters = parameters;
		return before;
	}

	/**
	 * パラメータの取得
	 * @return 現在のパラメータ
	 */
	public double[] getParameters() {
		return this.parameters;
	}


}