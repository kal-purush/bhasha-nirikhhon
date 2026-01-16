package helpers;

import org.testng.ITestResult;
import org.testng.reporters.ExitCodeListener;

public class CustomListener extends ExitCodeListener{

    @Override
    public void onTestFailure(ITestResult result) {
        Actions.createAttachment(result.getMethod().getMethodName());
    }

    @Override
    public void onTestStart(ITestResult result) {
        super.onTestStart(result);
    }

    @Override
    public void onTestSuccess(ITestResult result) {
        super.onTestSuccess(result);
    }
}