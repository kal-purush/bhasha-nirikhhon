package pages.Address;

import configuration.PageBase;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.testng.Assert;
import utils.CRUD.User;

public class Address extends PageBase {
    private AddressElements webElements;

    private Address(WebDriver driver){
        super(driver);
    }

    public static Address of(WebDriver driver){
        return new Address(driver);
    }

    //region Web Elements
    private By FullNameLabel() { return By.cssSelector(webElements.FULL_NAME_LABEL); }
    private By AddressLabel() { return By.cssSelector(webElements.ADDRESS_LABEL); }
    private By CityStateZipLabel() { return By.cssSelector(webElements.CITY_STATE_ZIP_LABEL); }
    private By CountryLabel() { return By.cssSelector(webElements.COUNTRY_LABEL); }
    private By MobilePhoneLabel() { return By.cssSelector(webElements.MOBILE_PHONE_LABEL); }
    private By ProceedToCheckoutButton() { return By.cssSelector(webElements.PROCEED_TO_CHECKOUT_BUTTON); }
    //endregion

    //region Actions
    public Address validateAddressInformation(User user){
        waitForElement(FullNameLabel());
        Assert.assertEquals(driver.findElement(FullNameLabel()).getText().toUpperCase(),
                (user.getFirstName() + " " + user.getLastName()).toUpperCase());
        Assert.assertEquals(driver.findElement(AddressLabel()).getText().toUpperCase(),
                user.getAddress().toUpperCase());
        Assert.assertEquals(driver.findElement(CityStateZipLabel()).getText().toUpperCase(),
                (user.getCity() + ", " + user.getState() + " " + user.getPostalCode()).toUpperCase());
        Assert.assertEquals(driver.findElement(CountryLabel()).getText().toUpperCase(),
                user.getCountry().toUpperCase());
        Assert.assertEquals(driver.findElement(MobilePhoneLabel()).getText(), user.getMobilePhone());
        return this;
    }
    public Address clickProceedToCheckoutButton(){
        click(ProceedToCheckoutButton());
        return this;
    }
    //endregion
}