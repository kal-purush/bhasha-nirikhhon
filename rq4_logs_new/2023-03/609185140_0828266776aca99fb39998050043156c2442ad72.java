package testng_basics;

import org.testng.ITestContext;
import org.testng.ITestListener;
import org.testng.ITestResult;

import java.util.Arrays;

public class ListnersClass implements ITestListener {

    @Override
    public void onTestStart(ITestResult result) {
        System.out.println("Test Started: "+result.getTestName());
    }

    @Override
    public void onTestSuccess(ITestResult result) {
        System.out.println("Test Success: "+result.getName());
    }

    @Override
    public void onTestFailure(ITestResult result) {
        System.out.println("Test Fail");
    }

    @Override
    public void onTestSkipped(ITestResult result) {
        ITestListener.super.onTestSkipped(result);
    }

    @Override
    public void onTestFailedButWithinSuccessPercentage(ITestResult result) {
        ITestListener.super.onTestFailedButWithinSuccessPercentage(result);
    }

    @Override
    public void onTestFailedWithTimeout(ITestResult result) {
        System.out.println("Timeout"+result.getTestName());
    }

    @Override
    public void onStart(ITestContext context) {
        System.out.println("Method Started: "+context.getFailedTests());
    }

    @Override
    public void onFinish(ITestContext context) {
        System.out.println("Method onFinish");
    }
}