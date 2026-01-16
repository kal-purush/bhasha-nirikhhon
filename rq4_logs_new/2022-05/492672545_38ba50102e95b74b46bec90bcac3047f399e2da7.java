package dvmProject;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;

import org.junit.Assert;
import org.junit.Before;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class UnitTests 
{
	DVM dvm = new DVM();
	String[] calcDVMInfo = new String[]{"VM_01", "22", "37"};
	int [] address = new int[] {37, 37};
    String[] closestDVM = new String[] {"VM_01", "71", "28"};
    ArrayList listOfDVM = new ArrayList<String[]>();
    
    @BeforeEach
    public void setUp() throws Exception 
    {
    	dvm.setCalcDVMInfo(calcDVMInfo);
    	dvm.setID(2);
    	dvm.setAddress(address);
    	

	    String[] example1 = new String[]{"VM_04", "34", "11"};
	    String[] example2 = new String[]{"VM_05", "66", "22"};
	    String[] example3 = new String[]{"VM_02", "55", "37"};
	    String[] example4 = new String[]{"VM_01", "71", "28"};
	 
	    listOfDVM.add(example1);
	    listOfDVM.add(example2);
	    listOfDVM.add(example3);
	    listOfDVM.add(example4);

    }
    
	@Test
	void test() 
	{
		Assert.assertArrayEquals(calcDVMInfo, dvm.getCalcDVMInfo());
		Assert.assertEquals(2, dvm.getID());
		Assert.assertArrayEquals(address, dvm.getAddress());
		//setDrinkList 안함
	    Assert.assertArrayEquals(closestDVM, dvm.calcClosestDVMLoc(listOfDVM));
	    Assert.assertEquals(true, dvm.cardPayment(100000));
	    Assert.assertEquals(7200, dvm.calcPrice("06", 6));
	    //setCurrentSellDrink 안함
	}

}