package playground.testng;

import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class Q3 {
  @Test(dataProvider = "dp")
  public void passedFailedTest(String n, String s) {
	  System.out.println(n+ " - " + "01-Starting executing passedFailedTest");
	  Assert.assertEquals(n,s,"Both values do not match");
	  System.out.println(n+ " - " + "01-Ending executing passedFailedTest");
  }
  @Test
  public void skippedTest() {
	  System.out.println("02-Starting executing skippedTest");
	  throw new SkipException("Skip this test as not yet implemented");
	  //System.out.println("02-Ending executing skippedTest");
  }
  @DataProvider
  public Object[][] dp() {
	  System.out.println("In the dataProvider method of class Q3");
    return new Object[][] {
      new Object[] { "a", "a" },
      new Object[] { "b", "a" },
    };
  }
}