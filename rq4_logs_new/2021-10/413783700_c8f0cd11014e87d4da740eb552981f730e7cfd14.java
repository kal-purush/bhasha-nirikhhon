package com.ss.ita.econews;

import static org.testng.Assert.assertTrue;

import org.openqa.selenium.support.Color;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.ss.ita.elements.Paragraph;
import com.ss.ita.pages.CreateNewsPage;
import com.ss.ita.pages.EcoNewsPage;
import com.ss.ita.pages.HomePage;
import static com.ss.ita.data.UserSignInData.*;


public class ContentTextAreaTest extends TestRuner {

	private CreateNewsPage createNewsPage;

	@BeforeClass
	public void beforeContentTextAreaTest() {
		createNewsPage = new HomePage(driver)
						.getHeader()
						.login(TARAS_KRASITSKYI.getEmail(), TARAS_KRASITSKYI.getPassword())
						.clickHomePageLink()
						.clickEcoNewsLink()
						.clickCreateNewsButton();
	}

	@Test(dataProvider = "dpTextAreaDescriptionIsRedTest")
	public void textAreaDescriptionIsRedTest(String shortText, String expectedTextColor) {
		createNewsPage = createNewsPage
						.setTextArea(shortText)
						.clickTextAreaDescription();
		assertTrue((Color
					.fromString(createNewsPage
								.getParagraph()
								.getColorTextParagraph())
								.asHex())
					.equals(expectedTextColor));
	}

	@Test(dataProvider = "dpTextAreaDescriptionIsRedTest")
	public void borderAreaDescriptionIsRedTest(String shortText, String expectedTextColor) {
		createNewsPage = createNewsPage
				.setTextArea(shortText)
				.clickTextAreaDescription();
		assertTrue((Color
				.fromString(createNewsPage
							.getTextArea()
							.getBorderColor())
							.asHex())
				.equals(expectedTextColor));
	}

}