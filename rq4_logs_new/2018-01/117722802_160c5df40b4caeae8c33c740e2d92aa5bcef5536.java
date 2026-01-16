package SeleniumListener;
import org.testng.ITestContext;
import org.testng.ITestListener;
import org.testng.ITestResult;

public class SeleniumTestListener implements ITestListener {

	@Override
	public void onTestStart(ITestResult result) {
		
		System.out.println("Started the Test cases:" + result);
	}

	@Override
	public void onTestSuccess(ITestResult result) {
		System.out.println("Test cases are passed:" + result);
		
	}

	@Override
	public void onTestFailure(ITestResult result) {
		System.out.println("Test cases are Failed:" + result);
		
	}

	@Override
	public void onTestSkipped(ITestResult result) {
		System.out.println("Test cases are Skipped:" + result);
		
	}

	@Override
	public void onTestFailedButWithinSuccessPercentage(ITestResult result) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onStart(ITestContext context) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onFinish(ITestContext context) {
		// TODO Auto-generated method stub
		
	}

	
}