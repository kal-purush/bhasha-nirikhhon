package net.coolblossom.lycee.machinelearning.classification.scale;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import net.coolblossom.lycee.machinelearning.classification.DataSet;

/**
 * 標準化を行うリスケーラー
 * @author ryouka0122@github
 *
 */
public class StandardizedRescaler implements DataSetRescaler {

	@Override
	public List<DataSet> rescale(List<DataSet> list) {
		int dimension = list.get(0).x.length;
		double[] sum = Stream.generate(()->0.0).limit(dimension).mapToDouble(x->x.doubleValue()).toArray();
		double[] sq = Stream.generate(()->0.0).limit(dimension).mapToDouble(x->x.doubleValue()).toArray();

		for(DataSet ds : list) {
			for(int i=0 ; i<dimension ; i++) {
				sum[i] += ds.x[i];
				sq[i] += ds.x[i] * ds.x[i];
			}
		}

		double[] sgm = new double[dimension];
		double[] ave = new double[dimension];
		for(int i=0 ; i<dimension ; i++) {
			sgm[i] = Math.sqrt(sq[i] - sum[i] * sum[i]);
			ave[i] = sum[i] / list.size();
		}

		List<DataSet> result = new ArrayList<DataSet>();
		for(DataSet ds : list) {
			DataSet dataSet = new DataSet(ds.y, ds.x);
			for(int i=0 ; i<dimension ; i++) {
				dataSet.x[i] = (dataSet.x[i] - ave[i]) / sgm[i];
			}
			result.add(dataSet);
		}
		return result;
	}

}