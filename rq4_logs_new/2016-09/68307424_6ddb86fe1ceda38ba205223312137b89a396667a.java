import static org.junit.Assert.*;

import java.io.FileNotFoundException;

import org.junit.Test;

public class UnitTesting {

	@Test
	public void test_Spelling() {
		boolean flag = false;
		SpellCheck sc = new SpellCheck();
		try {
			flag = sc.Checkker("apple");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		assertTrue(flag == true) ;
	}

}