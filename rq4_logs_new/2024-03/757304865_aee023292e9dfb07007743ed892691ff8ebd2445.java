package ui_beambox_automation;

import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import java.util.Random;

public class CancellationFlow {
	private WebDriver driver;
	private static final String NAV_LINK_XPATH = "//*[@id='navigation']/ul[1]/li[7]/a";
	private static final String SETTING_LINK_XPATH = "//*[@id='navigation']/ul[1]/li[7]/ul/li[1]/a";
	private static final String CANCEL_ACCOUNT_LINK_XPATH = "//*[@id='edit_user_6']/div[10]/div/a";
	private static final String PAUSE_SUBSCRIPTION_BUTTON_XPATH = "//*[@id='pause_subscription']/div/div/button";
	private static final String TWO_MONTH_RADIO_ID = "twoMonth";
	private static final String THREE_MONTH_RADIO_ID = "threeMonth";
	private static final String ACCEPT_DISCOUNT_BUTTON_XPATH = "//*[@id=\"subscription_discount\"]/div[1]/button";
	private static final String UPGRADE_TO_GROWTH_BUTTON_XPATH = "//*[@id=\"data-source\"]/div[2]/div/div/div/div/div[3]/div[2]/a";
	private static final String UPGRADE_TO_PRO_BUTTON_XPATH = "//*[@id=\"data-source\"]/div[2]/div/div/div/div/div[3]/div[3]/a";
	private static final String CONFIRM_PAUSE_SUBSCRIPTION_BUTTON_XPATH = "/html/body/div[6]/div/div[10]/button[1]";
	private static final String CONFIRM_DOWNGRADE_PLAN_BUTTON_XPATH = "//*[@id='downgrade_subscription']/div[1]/div/div[2]/button";
	private static final String DOWNGRADE_PLAN_BUTTON_XPATH = "//*[@id='downgrade_subscription']/div[2]/button";
	private static final String CANCEL_SUBSCRIPTION_INSTEAD_LINK_XPATH = "//*[@id='pause_subscription']/div/div/a";
	private static final String CANCEL_SUBSCRIPTION_INSTEAD_ON_PAUSE_PAGE_LINK_XPATH = "//*[@id='pause_subscription']/div/div/a";
	private static final String CANCEL_SUBSCRIPTION_INSTEAD_ON_DOWNGRADE_PAGE_LINK_XPATH = "//*[@id=\"downgrade_subscription\"]/div[2]/a";
	private static final String CANCEL_SUBSCRIPTION_INSTEAD_ON_DISCOUNT30_PAGE_LINK_XPATH = "//*[@id=\"subscription_discount\"]/div[1]/a";

	public CancellationFlow(WebDriver driver) {
		this.driver = driver;
	}

	public void cancellationFlowMain(int cancellationFlowOption) throws InterruptedException {
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
			if (cancellationFlowOption == 1) {
				pauseSubscription(randomNumber);
				Thread.sleep(6000);
				driver.close();
			}
		} catch (Exception e) {
			System.out.println("Something went wrong: " + e.getMessage());
		}

		try {
			if (cancellationFlowOption == 2) {
				Thread.sleep(5000);
				WebElement cancelSubscriptionInstead = driver
						.findElement(By.xpath(CANCEL_SUBSCRIPTION_INSTEAD_LINK_XPATH));
				cancelSubscriptionInstead.click();
				Thread.sleep(5000);
				try {
					downgradePlan("growth");
					Thread.sleep(3000);
					driver.close();
				} catch (Exception e) {
					System.out.println("Skiped the Downgrade Option");
					driver.close();
				}
			}
		} catch (Exception e) {
			System.out.println("Something went wrong: " + e.getMessage());
		}

		try {
			if (cancellationFlowOption == 3 || cancellationFlowOption == 4) {
				Thread.sleep(5000);
				WebElement cancelSubscriptionInsteadOnPasuePage = driver
						.findElement(By.xpath(CANCEL_SUBSCRIPTION_INSTEAD_ON_PAUSE_PAGE_LINK_XPATH));
				cancelSubscriptionInsteadOnPasuePage.click();

				try {
					Thread.sleep(3000);
					WebElement cancelInsteadOnDowngrade = driver
							.findElement(By.xpath(CANCEL_SUBSCRIPTION_INSTEAD_ON_DOWNGRADE_PAGE_LINK_XPATH));
					cancelInsteadOnDowngrade.click();
				} catch (Exception e) {
					System.out.println("Skipped Downgrade Page");
				}
				if (cancellationFlowOption == 4) {
					WebElement cancelSubscriptionInsteadOnDiscount30Page = driver
							.findElement(By.xpath(CANCEL_SUBSCRIPTION_INSTEAD_ON_DISCOUNT30_PAGE_LINK_XPATH));
					cancelSubscriptionInsteadOnDiscount30Page.click();
				}

				try {
					discount30Or50(randomNumber);
				} catch (Exception e) {
					System.out.println("You already availed the discount");
					driver.close();
				}
			}
		} catch (Exception e) {
			System.out.println("Something went wrong: " + e.getMessage());
		}

	}

	public void pauseSubscription(int randomNumber) throws InterruptedException {
		WebElement pauseSubscriptionButton = driver.findElement(By.xpath(PAUSE_SUBSCRIPTION_BUTTON_XPATH));

		if (randomNumber == 2) {
			WebElement twoMonthRadio = driver.findElement(By.id(TWO_MONTH_RADIO_ID));
			twoMonthRadio.click();
		} else if (randomNumber == 3) {
			WebElement threeMonthRadio = driver.findElement(By.id(THREE_MONTH_RADIO_ID));
			threeMonthRadio.click();
		}

		pauseSubscriptionButton.click();
		Thread.sleep(2000);
		WebElement confirmPauseSubscriptionButton = driver
				.findElement(By.xpath(CONFIRM_PAUSE_SUBSCRIPTION_BUTTON_XPATH));
		confirmPauseSubscriptionButton.click();
	}

	public void downgradePlan(String plan) {
		try {
			WebElement planRadio = driver.findElement(By.id(plan + "_plan"));
			planRadio.click();
		} catch (Exception e) {
			System.out.println("You are already on " + plan + " Plan");
			driver.close();
		}

		try {
			Thread.sleep(2000);
			WebElement downgradePlanButton = driver.findElement(By.xpath(DOWNGRADE_PLAN_BUTTON_XPATH));
			downgradePlanButton.click();
			Thread.sleep(5000);
			WebElement confirmDowngradePlanButton = driver.findElement(By.xpath(CONFIRM_DOWNGRADE_PLAN_BUTTON_XPATH));
			confirmDowngradePlanButton.click();
		} catch (Exception e) {
			System.out.println("Your are not Downgrade the Plan");
		}
	}

	public void discount30Or50(int randomNumber) throws InterruptedException {
		Thread.sleep(2000);
		WebElement acceptDiscount = driver.findElement(By.xpath(ACCEPT_DISCOUNT_BUTTON_XPATH));
		if (randomNumber == 1) {
			acceptDiscount.click();
		} else if (randomNumber == 2) {
			try {
				WebElement upgradeToGrowth = driver.findElement(By.xpath(UPGRADE_TO_GROWTH_BUTTON_XPATH));
				upgradeToGrowth.click();
			} catch (Exception e) {
				System.out.println(
						"You cannot upgrade to Growth as you are already on the Growth Plan. Availing the discount by clicking on 'Accept Offer' Button");
				acceptDiscount.click();
			}
		} else if (randomNumber == 3) {
			try {
				WebElement upgradeToPro = driver.findElement(By.xpath(UPGRADE_TO_PRO_BUTTON_XPATH));
				upgradeToPro.click();
			} catch (Exception e) {
				System.out.println(
						"You cannot upgrade to Pro as you are already on the Pro Plan. Availing the discount by clicking on 'Accept Offer' Button");
			}
		}
	}
}