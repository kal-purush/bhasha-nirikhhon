package hr.fer.zemris.optjava;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import hr.fer.zemris.optjava.data.Dataset;
import hr.fer.zemris.optjava.data.Sample;

public class Loader {

	public static Dataset loadData(String file) throws IOException {
		Path path = Paths.get(file);
		Dataset dataset = new Dataset();
		int dimX = -1;
		int dimY = -1;
		for (String line : Files.readAllLines(path)) {
			String[] s = line.split(":");
			if (dimX == -1 || dimY == -1) {
				dimX = s[0].split(",").length;
				dimY = s[1].split(",").length;
			}
			double[] input = new double[dimX];
			double[] output = new double[dimY];
			
			String[] values = s[0].substring(1, s[0].length()-1).split(",");
			for (int i = 0; i < dimX; ++i) {
				input[i] = Double.parseDouble(values[i]);
			}
			values = s[1].substring(1, s[1].length()-1).split(",");
			for (int i = 0; i < dimY; ++i) {
				output[i] = Double.parseDouble(values[i]);
			}
			dataset.add(new Sample(input, output));
		}
		return dataset;
	}
	
}