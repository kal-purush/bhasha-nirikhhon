package utils;

import com.aventstack.extentreports.ExtentTest;
import com.aventstack.extentreports.MediaEntityBuilder;
import constants.ProjectConstants;
import org.apache.commons.io.FileUtils;
import org.openqa.selenium.OutputType;
import org.openqa.selenium.TakesScreenshot;
import org.openqa.selenium.WebDriver;
import org.testng.ITestResult;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

public class Screenshot {
    private final String ERROR_MESSAGE = "Error Screenshot:";
    private WebDriver driver;
    private ExtentTest extentTest;

    private Screenshot(WebDriver driver){
        this.driver = driver;
    }

    public static Screenshot of(WebDriver driver){
        return new Screenshot(driver);
    }

    public Screenshot with(ExtentTest extentTest){
        this.extentTest = extentTest;
        return this;
    }

    public void takeScreenshot() throws IOException {
        File scrFile = ((TakesScreenshot)driver).getScreenshotAs(OutputType.FILE);
        String fileName = UUID.randomUUID().toString() + ".png";

        File targetFile = new File(ProjectConstants.REPORT_PATH, fileName);
        FileUtils.copyFile(scrFile, targetFile);

        if(extentTest != null){
            extentTest.fail(ERROR_MESSAGE, MediaEntityBuilder.createScreenCaptureFromPath(targetFile.getPath()).build());
        }
    }
}