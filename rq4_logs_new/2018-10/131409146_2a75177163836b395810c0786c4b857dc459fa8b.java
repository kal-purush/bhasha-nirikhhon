package Copy;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Parameters;

public class Basic {

	@Parameters({ "Environment" })
	@BeforeMethod
	public void EnvironmentSetup( String Environment) {

//		if (System.getProperty("env1").equalsIgnoreCase("prod")) {
//			System.out.println("Abhishek prod env");
//		}
//		
//		if (System.getProperty("env1").equalsIgnoreCase("QA")) {
//			System.out.println("Srii QA env");
//		}

		if (Environment.equalsIgnoreCase("Prod")) {
			String UrlP = "https://www.flipkart.com/";
			System.out.println("Production Environment");
		}
		else if (Environment.equalsIgnoreCase("QA")) {
			String UrlQA = "https://www.amazon.in/?tag=googinhydmabk-21&ref_=pd_mn_ABKror78";
			System.out.println("QA Environment");
		}

	}

	@AfterMethod
	public void TearDown() {
		System.out.println("TearDown");
	}

}