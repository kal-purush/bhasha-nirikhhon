package es.upm.miw.ecp1.test;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import es.upm.miw.ecp1.C32;

public class C32Test {
	private C32 c32;
	
	@Before
	public void before () {
		c32 = new C32();
	}
	
	@Test
	public void mA () {
		Assert.assertEquals(c32.mA(), "mA");
	}
}