package browser;

import com.codeborne.selenide.Configuration;
import utils.PropertyReader;

import static com.codeborne.selenide.Browsers.*;

public class SelenideConfiguration {
    public static void configureBrowser(String browser) {
        setUpBasicConfigure();

        switch (browser) {
            case "safari":
                Configuration.browser = SAFARI;
                break;
            case "firefox":
                Configuration.browser = FIREFOX;
                break;
            default:
                Configuration.browser = CHROME;
                break;
        }
    }
    public static void setUpBasicConfigure() {
        PropertyReader reader = new PropertyReader();
        Configuration.baseUrl = "urlBooking";
        Configuration.headless = false;
        Configuration.browserSize = "1000x1000";
        Configuration.timeout = 10000;
        Configuration.screenshots = false;
    }
}