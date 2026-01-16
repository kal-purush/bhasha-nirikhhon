package helpers;

import com.codeborne.selenide.Configuration;
import org.openqa.selenium.Capabilities;
import org.openqa.selenium.remote.RemoteWebDriver;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Properties;

import static com.codeborne.selenide.WebDriverRunner.getWebDriver;

public class EnvironmentWriter {
    private static Capabilities caps = ((RemoteWebDriver)getWebDriver())
            .getCapabilities();

    public static void writeEnvironmentProperties() {

        Properties envProp = new Properties();
        OutputStream outputStream = null;
        String path = System.getProperty("user.dir") + "/target/allure-results/";

        try {
            outputStream = new FileOutputStream(path + "environment.properties");

            envProp.setProperty("OS:", caps.getPlatform().name());
            envProp.setProperty("Browser Name", caps.getBrowserName());
            envProp.setProperty("Browser Version:", caps.getVersion());
            envProp.setProperty("Base URL:", Configuration.baseUrl);

            envProp.store(outputStream, "");

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (outputStream != null) {
                try {
                    outputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}