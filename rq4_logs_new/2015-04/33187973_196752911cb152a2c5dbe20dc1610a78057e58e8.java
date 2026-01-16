package calculeMat;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.apache.commons.math3.linear.*;

public class Calcule {

	
	public static void main(String[] args) {
		double[][] matrixData=new double[4][4];
		double[] vectorData=new double[4];
		String csvFile = "C:/Users/user/Documents/Simu/mobile/angles.csv";
		BufferedReader br = null;
		String line = "";
		String cvsSplitBy = ";";		
		double angle, x, y; 
		int t;
		try {
			br = new BufferedReader(new FileReader(csvFile));		
			br.readLine();
			int i =0;
			while ((line = br.readLine()) != null && i<4) {

				String[] data = line.split(cvsSplitBy);
				t = Integer.parseInt(data[0]);				
				angle = Double.parseDouble(data[2]);
				x = Double.parseDouble(data[4]);
				y = Double.parseDouble(data[5]);
				System.out.println("t="+t+" x="+x+" y="+y+" angle="+angle);
			
				matrixData[t-1][0]= Math.sin(angle);
				matrixData[t-1][1] = t*Math.sin(angle);
				matrixData[t-1][2] = -Math.cos(angle);
				matrixData[t-1][3] = -t*Math.cos(angle);
			
				vectorData[t-1] = Math.cos(angle)*y -Math.sin(angle)*x;
				i++;
			}
		
		
		RealMatrix m = MatrixUtils.createRealMatrix(matrixData);
		RealVector v = MatrixUtils.createRealVector(vectorData);
		DecompositionSolver solver = new LUDecomposition(m).getSolver();
		RealVector solution = solver.solve(v);
		
		System.out.println(solution);
			
		} catch (IOException e) {
			System.out.println("Erreur lecture fichier");
			e.printStackTrace();
		}	
			
			
	}

}

