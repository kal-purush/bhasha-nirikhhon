package pages;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class DataProvider_Page {

	@DataProvider(name="calling")
	public Object[][] getdata()
	{
		Object[][] data=new Object[4][2];
	
		return new  Object[][] {{"name","sername"},
			{"ram","prakash"},
			{"monika","dube"},
			{"amarjeet","pall"},
			{"puspa","kumari"}} ;
	}
	
	@Test(dataProvider = "calling")
	public void getter(String name , String sername)
	{
		System.out.println(name);
		System.out.println(sername);
	}
}