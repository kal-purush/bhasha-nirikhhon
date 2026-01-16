package pages.SignIn;

import com.github.javafaker.Faker;
import configuration.PageBase;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import utils.CRUD.User;

public class SignIn extends PageBase {
    private SignInElements webElements;

    private SignIn(WebDriver driver){
        super(driver);
    }

    public static SignIn of(WebDriver driver){
        return new SignIn(driver);
    }

    //region Web Elements
    private By EmailInput() { return By.cssSelector(webElements.EMAIL_INPUT); }
    private By CreateAccountButton() { return By.cssSelector(webElements.CREATE_ACCOUNT_BUTTON); }
    private By FirstNameInput() { return By.cssSelector(webElements.FIRST_NAME_INPUT); }
    private By LastNameInput() { return By.cssSelector(webElements.LAST_NAME_INPUT); }
    private By PasswordInput() { return By.cssSelector(webElements.PASSWORD_INPUT); }
    private By AddressInput() { return By.cssSelector(webElements.ADDRESS_INPUT); }
    private By CityInput() { return By.cssSelector(webElements.CITY_INPUT); }
    private By StateCombo() { return By.cssSelector(webElements.STATE_COMBO); }
    private By SelectedStateLabel() { return By.cssSelector(webElements.SELECTED_STATE_LABEL); }
    private By PostalCodeInput() { return By.cssSelector(webElements.POSTAL_CODE_INPUT); }
    private By CountryCombo() { return By.cssSelector(webElements.COUNTRY_COMBO); }
    private By SelectedCountryLabel() { return By.cssSelector(webElements.SELECTED_COUNTRY_LABEL); }
    private By MobilePhoneInput() { return By.cssSelector(webElements.MOBILE_PHONE_INPUT); }
    private By RegisterButton() { return By.cssSelector(webElements.REGISTER_BUTTON); }
    //endregion

    //region Actions
    public SignIn fillEmailInput(User user){
        fillString(EmailInput(), user.getEmail());
        return this;
    }
    public SignIn clickCreateAnAccountButton(){
        click(CreateAccountButton());
        return this;
    }
    public SignIn fillCreateAccountForm(User user){
        fillString(FirstNameInput(), user.getFirstName());
        fillString(LastNameInput(), user.getLastName());
        fillString(PasswordInput(), user.getPassword());
        fillString(AddressInput(), user.getAddress());
        fillString(CityInput(), user.getCity());
        selectRandOption(StateCombo());
        fillString(PostalCodeInput(), user.getPostalCode());
        selectRandOption(CountryCombo());
        fillString(MobilePhoneInput(), user.getMobilePhone());

        user.setState(driver.findElement(SelectedStateLabel()).getText());
        user.setCountry(driver.findElement(SelectedCountryLabel()).getText());

        return this;
    }
    public SignIn clickRegisterButton(){
        click(RegisterButton());
        return this;
    }
    //endregion
}