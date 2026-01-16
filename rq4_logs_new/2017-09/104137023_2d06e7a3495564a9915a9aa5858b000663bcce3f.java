package net.coolblossom.lycee.machinelearning.classification;

import java.util.stream.Stream;

import net.coolblossom.lycee.machinelearning.classification.kernels.InnerProduct;
import net.coolblossom.lycee.machinelearning.classification.kernels.Kernel;

/**
 * 分類器の基底クラス
 * @author ryouka0122@github
 *
 */
public abstract class AbstractClassifier implements Classifier {

	/** データ次数 */
	protected int dataDimension;

	/** データに係る重みパラメータ */
	protected double[] parameters;

	/** 予測計算に使う計算ロジック */
	protected Kernel kernel;

	/**
	 * コンストラクタ
	 * @param dataDimension データ次数（バイアス項含む）
	 */
	protected AbstractClassifier(int dataDimension) {
		this(dataDimension, new InnerProduct());
	}

	/**
	 * コンストラクタ
	 * @param dataDimension データ次数（バイアス項含む）
	 * @param kernel 予測に使用するカーネル
	 */
	protected AbstractClassifier(int dataDimension, Kernel kernel) {
		this.dataDimension = dataDimension;
		this.kernel = kernel;
		this.parameters = Stream.iterate(0.0, t->t).limit(dataDimension).mapToDouble(v->v).toArray();
	}

	/**
	 * 予測で使用するカーネルの設定
	 * @param kernel カーネル
	 */
	public void setKernel(Kernel kernel) {
		this.kernel = kernel;
	}

	/**
	 * 解析後のパラメータの取得
	 * @return
	 */
	public double[] getParameters() {
		return this.parameters;
	}

	/**
	 * 予測
	 * @param data 観測データ
	 * @return 予測値（ラベル）
	 */
	@Override
	public double predict(double[] data) {
		// 観測データと重みパラメータの内積  y = <w, x>
		return kernel.calc(parameters, data);
	}

}