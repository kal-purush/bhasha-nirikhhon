import io.appium.java_client.MobileElement;
import io.appium.java_client.TouchAction;
import io.appium.java_client.android.AndroidDriver;
import io.appium.java_client.android.AndroidElement;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.io.File;
import java.io.FileNotFoundException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;

import org.openqa.selenium.NoSuchElementException;
import org.openqa.selenium.remote.DesiredCapabilities;

import static io.appium.java_client.touch.offset.PointOption.point;

public class SampleTest {

    private AndroidDriver<AndroidElement> driver;
    double pass_rate = 0.0;
    String test_cases_csv_dir = "/Users/Mary/Documents/Fall22/CMPE187/Socratic/src/main/resources/test_cases.csv";
    String img_folder_dir = "/Users/Mary/Documents/Fall22/CMPE187/Socratic/src/main/resources/images";
    String output_file_csv_dir = "/Users/Mary/Documents/Fall22/CMPE187/Socratic/src/main/resources/output.csv"; //Auto create file if not there

    @Before
    public void setUp() throws MalformedURLException {
        DesiredCapabilities desiredCapabilities = new DesiredCapabilities();
        desiredCapabilities.setCapability("platformName", "Android");
        desiredCapabilities.setCapability("appium:platformVersion", "13");
        desiredCapabilities.setCapability("appium:deviceName", "Pixel 4");
        desiredCapabilities.setCapability("appium:appPackage", "com.google.socratic");
        desiredCapabilities.setCapability("appium:appActivity", "com.google.android.apps.education.bloom.app.home.HomeActivity");
        desiredCapabilities.setCapability("appium:noReset", true);
        desiredCapabilities.setCapability("appium:ensureWebviewsHavePages", true);
        desiredCapabilities.setCapability("appium:nativeWebScreenshot", true);
        desiredCapabilities.setCapability("appium:newCommandTimeout", 3600);
        desiredCapabilities.setCapability("appium:connectHardwareKeyboard", true);
        desiredCapabilities.setCapability("autoDismissAlerts", true);

        URL remoteUrl = new URL("http://127.0.0.1:4723/wd/hub");

        driver = new AndroidDriver(remoteUrl, desiredCapabilities);
    }



    @Test
    public void sampleTest() throws InterruptedException, FileNotFoundException {
        Scanner s = new Scanner(new File("/Users/Mary/Documents/Fall22/CMPE187/Socratic/src/main/resources/texttestcases.txt"));
        ArrayList<String> list = new ArrayList<String>();
        HashMap<String, Integer> chapterPass = new HashMap<String, Integer>();
        HashMap<String, Integer> chapterCount = new HashMap<String, Integer>();
        while (s.hasNextLine()){
            list.add(s.nextLine());
        }
        s.close();

        MobileElement el1 = (MobileElement) driver.findElementByAccessibilityId("Type a question");
        el1.click();
        double num_test_cases = list.size();
        double pass_count = 0.0;

        for (int j = 0; j < list.size(); j++) {
            String[] phrase = list.get(j).split(":");
            if (chapterCount.containsKey(phrase[1])) {
                chapterCount.put(phrase[1], chapterCount.get(phrase[1]) + 1);
            }
            else {
                chapterCount.put(phrase[1], 1);
            }
            Thread.sleep(1500);
            if (!isElementPresent("id","com.google.socratic:id/text_query")) {
                driver.launchApp();
                Thread.sleep(1500);
                MobileElement el13 = (MobileElement) driver.findElementByAccessibilityId("Type a question");
                el13.click();
                Thread.sleep(1500);
            }

            MobileElement el2 = (MobileElement) driver.findElementById("com.google.socratic:id/text_query");
            el2.sendKeys(phrase[0]);
            Thread.sleep(1500);
            TouchAction touchAction = new TouchAction(driver);
            touchAction.tap(point(986, 2059)).perform();
            Thread.sleep(1500);

            String res1 = "none";
            String res2 = "none";
            String res3 = "none";
            String res4 = "none";
            String res5 = "none";
            String res6 = "none";
            String res7 = "none";
            if (isElementPresent("id","com.google.socratic:id/youtube_title")) {
                MobileElement el5 = (MobileElement) driver.findElementById("com.google.socratic:id/youtube_title");
                res1 = el5.getText().toLowerCase();

            }
            if (isElementPresent("id","com.google.socratic:id/answer")) {
                MobileElement el6 = (MobileElement) driver.findElementById("com.google.socratic:id/answer");
                res2 = el6.getText().toLowerCase();
                MobileElement el10 = (MobileElement) driver.findElementById("com.google.socratic:id/question");
                res5 = el10.getText().toLowerCase();
                res5 = res5.substring(0,res5.length()-1);

            }
            if (isElementPresent("id","com.google.socratic:id/google_subtitle")) {
                MobileElement el7 = (MobileElement) driver.findElementById("com.google.socratic:id/google_subtitle");
                res3 = el7.getText().toLowerCase();
                MobileElement el8 = (MobileElement) driver.findElementById("com.google.socratic:id/google_title");
                res4 = el8.getText().toLowerCase();

            }
            if (isElementPresent("id","com.google.socratic:id/alertTitle")){
                MobileElement el8 = (MobileElement) driver.findElementById("android:id/button1");
                el8.click();
                res1 = "FAIL";
                res2 = "FAIL";
                res3 = "FAIL";
                res4 = "FAIL";
                res5 = "FAIL";
                res6 = "FAIL";
                res7 = "FAIL";
            }
            if (isElementPresent("id","com.google.socratic:id/explainers_result_title")) {
                MobileElement el11 = (MobileElement) driver.findElementById("com.google.socratic:id/explainers_result_title");
                res6 = el11.getText().toLowerCase();
            }
            if (isElementPresent("id","com.google.socratic:id/solution_view")) {
                MobileElement el12 = (MobileElement) driver.findElementById("com.google.socratic:id/solution_view");
                res7 = el12.getText().toLowerCase();
            }

            pass_rate = 0;
                for (int i = 2; i < phrase.length; i++) {
                    if (res1.contains(phrase[i]) || res2.contains(phrase[i]) || res3.contains(phrase[i]) || res4.contains(phrase[i]) || res5.contains(phrase[i]) || res6.contains(phrase[i]) || res7.contains(phrase[i])) {
                        ++pass_rate;
                    }
                    else if (res1.startsWith(phrase[i]) || res2.startsWith(phrase[i]) || res3.startsWith(phrase[i]) || res4.startsWith(phrase[i]) || res5.startsWith(phrase[i]) || res6.startsWith(phrase[i]) || res7.startsWith(phrase[i])) {
                        pass_rate += 2;
                    }
                }
                if (phrase[0].toLowerCase().contains(res1) || phrase[0].toLowerCase().contains(res2) || phrase[0].toLowerCase().contains(res3.substring(0,res3.length()-1)) || phrase[0].toLowerCase().contains(res4.substring(0,res4.length()-1)) || phrase[0].toLowerCase().contains(res5.substring(0,res5.length()-1)) || phrase[0].toLowerCase().contains(res6) || phrase[0].toLowerCase().contains(res7) ){
                    pass_rate = phrase.length;
                }
                double avg = pass_rate / phrase.length;
                if (pass_rate >= 1) {

//                    System.out.println("PASS");
                    ++pass_count;
                    if (chapterPass.containsKey(phrase[1])) {
                        chapterPass.put(phrase[1], chapterPass.get(phrase[1]) + 1);
                    }
                    else {
                        chapterPass.put(phrase[1], 1);
                    }

                } else {
//                    System.out.println("TEST FAILED");
                }

            Thread.sleep(1500);
            touchAction.tap(point(77, 154)).perform();
            Thread.sleep(1500);

            }
        System.out.println(chapterPass);
        double overall_pass_rate = pass_count/num_test_cases;
        System.out.println(overall_pass_rate);
        HashMap<String, Double> passByChapter = new HashMap<>();
        for (HashMap.Entry<String, Integer> entry : chapterCount.entrySet()) {
            double total = entry.getValue();
            double divider = chapterPass.get(entry.getKey());
            passByChapter.put(entry.getKey(), divider/total);
        }
        System.out.println(passByChapter);
    }

    @After
    public void tearDown() {
        driver.quit();
    }
protected boolean isElementPresent(String type, String by) {

    try {
        if (type == "id"){
            driver.findElementById(by);
        }
        if (type == "xpath"){
            driver.findElementByXPath(by);
        }

        return true;

    } catch (NoSuchElementException e) {

        return false;

    }
}
}
