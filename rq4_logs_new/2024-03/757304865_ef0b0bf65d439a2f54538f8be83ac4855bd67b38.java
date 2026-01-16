package ui_beambox_automation;

import java.util.Random;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;

public class CancellatioFlow {
    private WebDriver driver;
    private static final String NAV_LINK_XPATH = "//*[@id='navigation']/ul[1]/li[7]/a";
    private static final String SETTING_LINK_XPATH = "//*[@id='navigation']/ul[1]/li[7]/ul/li[1]/a";
    private static final String CANCEL_ACCOUNT_LINK_XPATH = "//*[@id='edit_user_6']/div[10]/div/a";
    private static final String PAUSE_SUBSCRIPTION_BUTTON_XPATH = "//*[@id='pause_subscription']/div/div/button";
    private static final String TWO_MONTH_RADIO_ID = "twoMonth";
    private static final String THREE_MONTH_RADIO_ID = "threeMonth";

    public CancellatioFlow(WebDriver driver) {
        super();
        this.driver = driver;
    }

    public WebDriver getDriver() {
        return driver;
    }

    public void setDriver(WebDriver driver) {
        this.driver = driver;
    }

    public void cancellationFlowMain(int cancellatioFlowOption) throws InterruptedException {
        Random random = new Random();
        int randomNumber = random.nextInt(3) + 1;
        System.out.println(randomNumber);

        Thread.sleep(5000);
        driver.findElement(By.xpath(NAV_LINK_XPATH)).click();

        Thread.sleep(2000);
        driver.findElement(By.xpath(SETTING_LINK_XPATH)).click();

        Thread.sleep(2000);
        driver.findElement(By.xpath(CANCEL_ACCOUNT_LINK_XPATH)).click();

        try {
	    	if (cancellatioFlowOption == 1) {
	            pauseSubscription(randomNumber);
	            Thread.sleep(6000);
	            driver.close();
	        }
        } catch(Exception e) {
        	System.out.println("Some Thing went wrong "+e.getLocalizedMessage());
        }

        try {
        	if (cancellatioFlowOption == 2) {
        		Thread.sleep(5000);
                WebElement cancelSubscriptionInstead = driver.findElement(By.xpath("//*[@id='pause_subscription']/div/div/a"));
                cancelSubscriptionInstead.click();
                Thread.sleep(5000);
                downgradePlan("growth");
                Thread.sleep(3000);
                driver.close();
            }
        } catch (Exception e) {
        	System.out.println("Some Thing went wrong "+e.getLocalizedMessage());
        }

    }

    public void pauseSubscription(int randomNumber) throws InterruptedException {
        Thread.sleep(5000);
        WebElement pauseSubscriptionButton = driver.findElement(By.xpath(PAUSE_SUBSCRIPTION_BUTTON_XPATH));

        if (randomNumber == 2) {
            WebElement twoMonthRadio = driver.findElement(By.id(TWO_MONTH_RADIO_ID));
            twoMonthRadio.click();
        } else if (randomNumber == 3) {
            WebElement threeMonthRadio = driver.findElement(By.id(THREE_MONTH_RADIO_ID));
            threeMonthRadio.click();
        }

        Thread.sleep(2000);
        pauseSubscriptionButton.click();
        Thread.sleep(2000);
        WebElement confirmPauseSubscriptionButton = driver.findElement(By.xpath("/html/body/div[6]/div/div[10]/button[1]"));
        confirmPauseSubscriptionButton.click();
    }

    public void downgradePlan(String plan) {
        if (plan.equalsIgnoreCase("essential")) {
            try {
                WebElement essentialPlanRadio = driver.findElement(By.id("essential_plan"));
                essentialPlanRadio.click();
            } catch (Exception e) {
                System.out.println("You are Already on Essential Plan");
                driver.close();
            }
        } else if (plan.equalsIgnoreCase("growth")) {
            try {
                WebElement growthPlanRadio = driver.findElement(By.id("growth_plan"));
                growthPlanRadio.click();
            } catch (Exception e) {
                System.out.println("You are Already on Growth Plan");
                driver.close();
            }
        }

        try {
            Thread.sleep(2000);
            WebElement downgradePlanButton = driver.findElement(By.xpath("//*[@id='downgrade_subscription']/div[2]/button"));
            downgradePlanButton.click();
            Thread.sleep(5000);
            WebElement confirmDowngradePlanButton = driver.findElement(By.xpath("//*[@id='downgrade_subscription']/div[1]/div/div[2]/button"));
            confirmDowngradePlanButton.click();
        } catch (Exception e) {
            System.out.println("Your are not Downgrade the Plan");
        }
    }
}