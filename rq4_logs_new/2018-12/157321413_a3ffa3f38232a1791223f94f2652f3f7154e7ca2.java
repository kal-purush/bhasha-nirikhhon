package ProjectTesting;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import edu.neu.info6205.project1.Stage;

class MapTest {

	@Test
	void testlength() {
		Stage map = new Stage();
		// will generate a 10*10 map and with 50 garbage
		int[][] maparray = map.getStage();
		assertEquals(10, maparray.length);
		assertEquals(10, maparray[0].length);
	}

	@Test
	void testGarbage() {
		Stage map = new Stage();
		// will generate a 10*10 map and with 50 garbage
		int[][] maparray = map.getStage();
		int count = 0;
		for (int i = 0; i < maparray.length; i++) {
			for (int j = 0; j < maparray[i].length; j++) {
				if (maparray[i][j] == 1) {
					count++;
				}
			}
		}
		assertEquals(50, count);
	}

}