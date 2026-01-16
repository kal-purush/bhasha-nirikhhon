package com.xmlangel.base;

import com.aventstack.extentreports.ExtentReports;
import com.aventstack.extentreports.ExtentTest;
import com.aventstack.extentreports.MediaEntityBuilder;
import com.aventstack.extentreports.Status;
import com.aventstack.extentreports.markuputils.ExtentColor;
import com.aventstack.extentreports.markuputils.Markup;
import com.aventstack.extentreports.markuputils.MarkupHelper;
import com.aventstack.extentreports.reporter.ExtentSparkReporter;
import com.aventstack.extentreports.reporter.configuration.Theme;
import org.apache.commons.io.FileUtils;
import org.openqa.selenium.OutputType;
import org.openqa.selenium.PageLoadStrategy;
import org.openqa.selenium.TakesScreenshot;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.chrome.ChromeOptions;
import org.slf4j.Marker;
import org.testng.Assert;
import org.testng.ITestResult;
import org.testng.SkipException;
import org.testng.annotations.*;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.concurrent.TimeUnit;

public class NewTest {

    ExtentReports extent = new ExtentReports();
    ExtentTest extentTest;
    ExtentSparkReporter reporter = new ExtentSparkReporter("Extentreport.html");
    WebDriver driver;
    //Properties
    public static final String WEB_DRIVER_ID = "webdriver.chrome.driver";
    public static final String WEB_DRIVER_PATH = "./chromedriver";


    @Test
    public void f1() {
        extentTest = extent.createTest("Launch Browser and website").assignAuthor("dicky")
                .assignCategory("smoke Test").assignDevice("Chrome");
        extentTest.log(Status.PASS, "user lauched website");
        extentTest.pass("user lauched website verifyed");
    }

    @Test
    public void f2() {
        extentTest = extent.createTest("Verify login").assignAuthor("dicky")
                .assignCategory("smoke Test").assignDevice("Chrome");
        extentTest.log(Status.PASS, "user lauched website");
        ;
        extentTest.info("alerts displaying");
        extentTest.pass("User logged into application");
        extentTest.warning("Reset password alerts displaying");
    }

    @Test
    public void f3() {
        extentTest = extent.createTest("Verify Dashboard").assignAuthor("gq")
                .assignCategory("monky Test").assignDevice("Chrome");
        extentTest.log(Status.PASS, "user lauched website");
        ;
        extentTest.skip("Verify dashboard skipped");

    }

    @Test
    public void f4() {
        extentTest = extent.createTest("Verify user send emails from test application").assignAuthor("cam")
                .assignCategory("function Test").assignDevice("IE");
        extentTest.log(Status.PASS, "user lauched website");
        ;
        extentTest.fail("Uable to email due to server down time");

    }

    @Test
    public void f5() {
        extentTest = extent.createTest("Verify reports and analytics").assignAuthor("나야")
                .assignCategory("smoke Test").assignDevice("FF");
        extentTest.log(Status.PASS, "user lauched website");
        ;
        extentTest.fail("reports getting crashed");

    }

    @Test
    public void f6() {
        extentTest = extent.createTest("Verify logout feature").assignAuthor("dicky")
                .assignCategory("smoke Test").assignDevice("firefox");
        extentTest.log(Status.PASS, "user lauched website");
        ;
        extentTest.pass("user logged out from application");
        extentTest.info("User is redirected to login page");

    }

    @Test
    public void f7() {
        extentTest = extent.createTest("Sucessful Test").assignDevice("Chrome");
    }

    @Test
    public void f8() {
        extentTest = extent.createTest("Failed Test");
        Assert.fail("Executing Failed Test Method ");
    }

    @Test
    public void f9() {
        extentTest = extent.createTest("Skipped Test");
        throw new SkipException("Executing Skipped Test Method");
    }

    @AfterMethod
    public void afterMethod(ITestResult result) {
        String methodName = result.getMethod().getMethodName();
        if (result.getStatus() == ITestResult.FAILURE) {
            String exceptionMessage = Arrays.toString(result.getThrowable().getStackTrace());
            extentTest.fail("<details><summary><b><font color=red>Exception Occured, click to see details : "
                    + "</font></b></summary>" + exceptionMessage.replaceAll(",", "<br>") + "</details> \n");
            String path = takeScreenshot(result.getMethod().getMethodName());
            try {
                extentTest.fail("<b><font color=red>" + "Screenshot of failure" + "</font></b></summary>" , MediaEntityBuilder.createScreenCaptureFromPath(path).build());
            } catch (Exception e) {
                extentTest.fail("Test Failed, cannot attach screenshot");
            }
            String longText = "<b>Test Method " + methodName + " Failed</b>";
            Markup m = MarkupHelper.createLabel(longText, ExtentColor.RED);
            extentTest.log(Status.FAIL, m);
        }
        else if (result.getStatus() == ITestResult.SUCCESS){
            String path = takeScreenshot(result.getMethod().getMethodName());
            try {
                extentTest.pass("<b><font color=green>" + "Screenshot of Success" + "</font></b></summary>" , MediaEntityBuilder.createScreenCaptureFromPath(path).build());
            } catch (Exception e) {
                extentTest.pass("Test SuccessFailed, cannot attach screenshot");
            }
            String longText = "<b>Test Method " + methodName + " Sucessful</b>";
            Markup m = MarkupHelper.createLabel(longText, ExtentColor.GREEN);
            extentTest.log(Status.PASS, m);
        }
        else if (result.getStatus() == ITestResult.SKIP){
            String longText = "<b>Test Method " + methodName + "Skipped</b>";
            Markup m = MarkupHelper.createLabel(longText, ExtentColor.YELLOW);
            extentTest.log(Status.SKIP, m);
        }
    }

    /**
     * webdriver 스크린샷찍기
     * 실패 했을 경우에 스크린샷을 찍기위해 만들어놓음.
     * @param methodName
     * @return
     */
    public String takeScreenshot(String methodName) {
        String fimeName = getScreenshotName(methodName);
        String directory = System.getProperty("user.dir")+ "/screenshots/";
        new File(directory).mkdirs();
        String path = directory + fimeName;
        try{
            File screenshot = ((TakesScreenshot)driver).getScreenshotAs(OutputType.FILE);
            FileUtils.copyFile(screenshot, new File(path));
            System.out.println("*****************************");
            System.out.println("Screenshot" + path);
            System.out.println("*****************************");
        }catch (Exception e){
            e.printStackTrace();
        }
        return path;
    }

    public static String getScreenshotName(String methodName){
        Date d = new Date();
        String fimeName = methodName + "_" + d.toString().replace(":", "_").replace(" ", "_") + ".png";
        return fimeName;
    }

    @BeforeClass
    public void beforeClass() {

        reporter.config().setEncoding("utf-8");
        reporter.config().setTheme(Theme.STANDARD);
        reporter.config().setDocumentTitle("Socar Automation");
        reporter.config().setReportName("Android Test Results");

        ChromeOptions options = new ChromeOptions();
        //페이지가 로드될 때까지 대기
        //Normal: 로드 이벤트 실행이 반환 될 때 까지 기다린다.
        options.setPageLoadStrategy(PageLoadStrategy.NORMAL);
        //System Property SetUp
        System.setProperty(WEB_DRIVER_ID, WEB_DRIVER_PATH);
        driver = new ChromeDriver(options);
        driver.get("https://xmlangel.github.io");
    }

    @AfterClass
    public void afterTest() {
        driver.quit();
        extent.flush();
    }

}