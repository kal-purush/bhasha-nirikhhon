package test.java.DemoQa.tests;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import test.java.DemoQa.foundation.DemoqaTestBase;
import test.java.DemoQa.pageObjects.DemoqaHomePage;
import test.java.DemoQa.pageObjects.DemoqaLinksPage;
import test.java.DemoQa.pageObjects.DemoqaRadioButtonPage;


public class DemoqaTest1 extends DemoqaTestBase {
	
	
	@Test
	public void canClickSubmitAndMatchTextInTextBoxPage() {
	//String expectUrl = "https://ampeg.com/index.html";

		new DemoqaHomePage(driver, baseUrl)
		.clickElementsPage()
		.clickTestBoxPage()
		.enterTextInTextBox()
		.clickSubmitButton();
		
	}

	@Test
	public void canClickRadioButoonAndMatchTextInRadioPage() {
		String clickYes = "Yes";
		String clickImpressive = "Impressive";
		
		new DemoqaHomePage(driver, baseUrl)
		.clickElementsPage()
		.clickRadioButtonPage()
		.clickYesButton()
		.clickImpressiveButton();
		
		assertEquals(DemoqaRadioButtonPage.clickYesOutputText,clickYes);
		assertEquals(DemoqaRadioButtonPage.clickImpressiveOutPutText,clickImpressive);
	}
	@Test
	public void canClickHomeLink() throws InterruptedException {// two works
		new DemoqaHomePage(driver, baseUrl)
		.clickElementsPage()
		.clickLinksPage()
		.clickHomeHyperLink()
		.clickNewPageJoinNowButton()
		.clickCreatedHyperLink();
	}
	@Test
	public void canVisibleAfterFiveSecondInDynamicPropertiesPage() {
		new DemoqaHomePage(driver, baseUrl)
		.clickElementsPage()
		.clickLinksPage().clickJoinpage();
	
	}

}