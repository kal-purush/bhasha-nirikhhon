package ebloodbanking.pages.collection;

import ebloodbanking.actions.PageActions;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.support.PageFactory;

public class AddNewDonor {
    private PageActions pageActions;


    public AddNewDonor(WebDriver driver) {
        PageFactory.initElements(driver, this);
        pageActions = new PageActions(driver);
    }

    /** sugar syntax */
    public AddNewDonor and() {
        return this;
    }

    public String getTitle() {
        return pageActions.getPageTitle();
    }
}