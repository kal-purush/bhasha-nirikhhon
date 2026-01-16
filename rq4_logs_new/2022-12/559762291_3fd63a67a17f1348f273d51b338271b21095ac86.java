package page_library;

import base.BasePage;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.PageFactory;

public class HomePage extends BasePage {

    @FindBy(xpath = "//li[@class = 'sports menu-soccer']/a")
    public WebElement soccerButton;

    @FindBy(xpath = "//div[@id = 'submenu-sportsmenusoccer']/ul[1]/li/a[@href='/soccer/']")
    public WebElement soccerHomeButton;


    public HomePage() {
        PageFactory.initElements(driver, this);
    }

    public SoccerPage navigateToSoccerPage() {
        safeClickOnElement(soccerButton);
        if(isElementVisible(soccerHomeButton)) {
            clickOnElement(soccerHomeButton);
        }
    return new SoccerPage();
    }
}