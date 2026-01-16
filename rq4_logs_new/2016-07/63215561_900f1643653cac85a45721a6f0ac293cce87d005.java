package session4.assignment1;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

public class TestTowerOfHanoi {
	
 TowerOfHanoi towerOfHanoi;
 List<String> outputList1;
 List<String> outputList2;

 
 
	@Before
	public void setUp() throws Exception {
		towerOfHanoi = new TowerOfHanoi();
		outputList1 = new ArrayList<String>();                              
		outputList2 = new ArrayList<String>();
		
		outputList1.add("Move Disk 1 from A to C");
		outputList1.add("Move Disk 2 from A to B");
		outputList1.add("Move Disk 1 from C to B");
		outputList1.add("Move Disk 3 from A to C");
		outputList1.add("Move Disk 1 from B to A");
		outputList1.add("Move Disk 2 from B to C");
		outputList1.add("Move Disk 1 from A to C");
		
		outputList2.add("Move Disk 1 from A to B");
		outputList2.add("Move Disk 2 from A to C");
		outputList2.add("Move Disk 1 from B to C");
		
		
	}

	@Test
	public void testGetPermutations() {
		
		assertEquals(outputList1, towerOfHanoi.getTowerSolution(3, "A", "B", "C"));
		TowerOfHanoi.outputList.clear();
		assertEquals(outputList2, towerOfHanoi.getTowerSolution(2, "A", "B", "C"));
		
	}
}