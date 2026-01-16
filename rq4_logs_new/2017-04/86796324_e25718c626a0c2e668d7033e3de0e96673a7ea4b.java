package test;

import static org.junit.Assert.*;

import org.junit.Test;

import com.hust.sonly.Inverse;

public class InverseTest {

	@Test
	public void test() throws Exception {
		double[][] det = {{0,1,2},{1,1,4},{2,-1,0}};
		
		
		System.out.println(Inverse.isReversible(det));
		System.out.println(Inverse.determinant(det));
		
		det = Inverse.inverse(det);
		for(int i=0;i<det.length;i++) {
			for(int j=0;j<det[0].length;j++)
				System.out.print(det[i][j]+" ");
			System.out.println();
		}
	}

}