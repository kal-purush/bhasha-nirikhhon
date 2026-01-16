package com.ss.ita.econews;

import com.ss.ita.pages.CreateNewsPage;
import com.ss.ita.pages.HomePage;
import org.openqa.selenium.support.Color;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.testng.asserts.SoftAssert;

import static com.ss.ita.data.UserSignInData.TARAS_KRASITSKYI;
import static org.testng.Assert.assertTrue;


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
        String colorTextParagraph = Color.fromString(createNewsPage.getParagraph()
                        .getColorTextParagraph())
                .asHex();


        softAssert.assertEquals(colorTextParagraph, expectedTextColor);

        softAssert.assertEquals(Color
                .fromString(createNewsPage
                        .getTextArea()
                        .getBorderColor())
                .asHex(), expectedTextColor);
        softAssert.assertAll();
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