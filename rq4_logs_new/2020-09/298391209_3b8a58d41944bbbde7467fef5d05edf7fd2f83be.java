package pages.MenuLoggedIn;

import configuration.PageBase;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;

public class MenuLoggedIn extends PageBase {
    private MenuLoggedInElements webElements;

    private MenuLoggedIn(WebDriver driver){
        super(driver);
    }

    public static MenuLoggedIn of(WebDriver driver){
        return new MenuLoggedIn(driver);
    }

    //region Web Elements
    private By LogoutButton() { return By.cssSelector(webElements.LOGOUT_BUTTON); }
    //endregion

    //region Actions
    public MenuLoggedIn clickLogoutButton(){
        click(LogoutButton());
        return this;
    }
    //endregion
}