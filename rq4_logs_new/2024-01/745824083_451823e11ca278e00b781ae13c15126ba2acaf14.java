package base;

import static utils.CommonUtils.takeScreenShotAndReturnPath;

import java.io.File;
import java.lang.reflect.Method;
import java.net.UnknownHostException;

import org.testng.ITestResult;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;

import com.aventstack.extentreports.ExtentReports;
import com.aventstack.extentreports.ExtentTest;
import com.aventstack.extentreports.MediaEntityBuilder;
import com.aventstack.extentreports.Status;
import com.aventstack.extentreports.markuputils.ExtentColor;
import com.aventstack.extentreports.markuputils.MarkupHelper;
import com.aventstack.extentreports.reporter.ExtentSparkReporter;
import com.aventstack.extentreports.reporter.configuration.Theme;

public class BaseTestForApiAutomation {

	public static ExtentSparkReporter sparkReporter;
	public static ExtentReports extentReports;
	public static ExtentTest api_logger;

	@BeforeTest
	public void setupReport() {
		String hostname = "";
		try {
			hostname = java.net.InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		String username = System.getProperty("user.name");
		sparkReporter = new ExtentSparkReporter(
				System.getProperty("user.dir") + File.separator + "src" + File.separator + "test" + File.separator
						+ "resources" + File.separator + "Reports" + File.separator + "API_AutomationReport.html");
		extentReports = new ExtentReports();
		extentReports.attachReporter(sparkReporter);
		sparkReporter.config().setTheme(Theme.DARK);
		extentReports.setSystemInfo("Hostname", hostname);
		extentReports.setSystemInfo("Username", username);
		sparkReporter.config().setDocumentTitle("Automation Reports");
		sparkReporter.config().setReportName("API Automation Test Results by Sharath");

	}

	@BeforeMethod
	public void beforeMethodMethod(Method testMethod) {
		api_logger = extentReports.createTest(testMethod.getName());
		api_logger.log(Status.INFO, "Running Method " + testMethod.getName());
	}

	@AfterMethod
	public void afterMethodMethod(ITestResult result) {
		if (result.getStatus() == ITestResult.FAILURE) {
			api_logger.log(Status.FAIL,
					MarkupHelper.createLabel(result.getName() + " - Test Case Failed ", ExtentColor.RED));
			api_logger.log(Status.FAIL, MarkupHelper.createLabel(result.getThrowable() + "", ExtentColor.RED));
			api_logger.log(Status.FAIL,
					MediaEntityBuilder.createScreenCaptureFromPath(takeScreenShotAndReturnPath()).build());
		} else if (result.getStatus() == ITestResult.SKIP) {
			api_logger.log(Status.FAIL,
					MarkupHelper.createLabel(result.getName() + " - Test Case Skipped ", ExtentColor.ORANGE));
		} else if (result.getStatus() == ITestResult.SUCCESS) {
			api_logger.log(Status.PASS,
					MarkupHelper.createLabel(result.getName() + " - Test Case PASS ", ExtentColor.GREEN));
		}
	}

	@AfterTest
	public void afterTestMethod() {
		extentReports.flush();
	}
}