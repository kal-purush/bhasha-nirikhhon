package kz.TELE2.pages;

import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.PageFactory;

public class HomePage {
    public WebDriver driver;

    public HomePage(WebDriver driver) {
        PageFactory.initElements(driver, this);
        this.driver = driver;
    }

    @FindBy(xpath = "//*[@id=\"app\"]/div/div[1]/div/div[3]/div/nav/li[4]")
    private WebElement personalAreaButton; //элемент главной страницы, кнопка "Личный кабинет"

    public void clickPersonalAreaButton (){ //кнопка "Личный кабинет", на главной странице
        personalAreaButton.click();
    }
}