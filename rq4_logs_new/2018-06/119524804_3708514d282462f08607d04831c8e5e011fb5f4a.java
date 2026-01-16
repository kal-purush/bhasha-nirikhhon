package db;

import static org.junit.Assert.*;

import org.junit.Test;

import dbupdate.InsertP;

public class InsertTest {

	@Test
	public void InsertP() {
		InsertP in = new InsertP();
		assertNotNull(in);
	}
}