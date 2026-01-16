package net.coolblossom;

import java.util.ArrayList;
import java.util.List;

import net.coolblossom.lycee.common.DataSet2D;
import net.coolblossom.lycee.deeplearning.ConvolutionalNeuralNetwork;

/**
 * CNNのサンプルプログラム
 * @author ryouka0122@github
 *
 */
public class SampleCNN extends SampleProgram {

	String EXPECT_DATA1 = "1";
	String[] TRAIN_DATA1 = {
			"00000100000", "00000100000", "00000100000",
			"00000100000", "00000100000", "00000100000",
			"00000100000", "00000100000", "00000100000",
			"00000100000", "00000100000",
	};

	String EXPECT_DATA2 = "1";
	String[] TRAIN_DATA2 = {
			"00001110000", "00001110000", "00001110000",
			"00001110000", "00001110000", "00001110000",
			"00001110000", "00001110000", "00001110000",
			"00001110000", "00001110000",
	};

	String EXPECT_DATA3 = "0";
	String[] TRAIN_DATA3 = {
			"00000000000",
			"00000000000",
			"00000000000",
			"00000000000",
			"00000000000",
			"11111111111",
			"00000000000",
			"00000000000",
			"00000000000",
			"00000000000",
			"00000000000",
	};

	String EXPECT_DATA4 = "0";
	String[] TRAIN_DATA4 = {
			"00000000000", "00000000000", "00000000000",
			"00000000000", "11111111111", "11111111111",
			"11111111111", "00000000000", "00000000000",
			"00000000000", "00000000000",
	};


	List<DataSet2D> SAMPLE_DATA = new ArrayList<DataSet2D>() {{
		add(convertDataSet2D(EXPECT_DATA1, TRAIN_DATA1));
		add(convertDataSet2D(EXPECT_DATA2, TRAIN_DATA2));
		add(convertDataSet2D(EXPECT_DATA3, TRAIN_DATA3));
		add(convertDataSet2D(EXPECT_DATA4, TRAIN_DATA4));
	}

	private DataSet2D convertDataSet2D(String sy, String[] sx) {

		double y = Double.parseDouble(sy);
		List<Double> list = new ArrayList<Double>();
		for(String s : sx) {
			for(char c : s.toCharArray()) {
				list.add(Double.parseDouble(""+c));
			}
		}

		return new DataSet2D(y, sx.length,
				list.stream().mapToDouble(d->d.doubleValue()).toArray());

	}};


	@Override
	void execute() {
		ConvolutionalNeuralNetwork network = new ConvolutionalNeuralNetwork(11, 3, 2, 3, 3, 3);

		network.setLearningRate(10.0);

		network.train(SAMPLE_DATA);

		for(DataSet2D ds : SAMPLE_DATA) {
			double result = network.predict(ds.x, ds.y);
			System.out.println(ds.y + ":" + result);
		}

	}

}