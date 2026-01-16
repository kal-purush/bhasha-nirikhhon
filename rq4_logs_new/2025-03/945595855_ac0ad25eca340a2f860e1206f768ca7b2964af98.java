import io.qameta.allure.junit4.DisplayName;
import model_page_object.MainPage;
import model_page_object.PageAuthorization;
import model_page_object.PageRecoveryPassword;
import model_page_object.PageRegistration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.openqa.selenium.WebDriver;
import support.InitialData;

public class AuthorizationUserTest {

    private ClientAPI clientAPI;
    private static WebDriver driver;

    @Before
    public void setUp() {
        clientAPI = new ClientAPI(); //инициализация клиента api
        clientAPI.createNewUser(InitialData.REGISTRATION_EMAIL, InitialData.REGISTRATION_PASSWORD, InitialData.REGISTRATION_NAME); //создание пользователя
        driver = Browser.getWebDriver(InitialData.BROWSER_NAME); //инициализация вебдрайвера
    }

    @Test
    @DisplayName("авторизация пользователя через кнопку \"Войти в аккаунт\"")
    public void authorizationWithButtonLoginAccount() {
        driver.get(InitialData.MAIN_PAGE_URL);
        MainPage mainPage = new MainPage(driver);
        PageAuthorization pageAuthorization = new PageAuthorization(driver);

        mainPage.clickOnButtonLogInAccount(); //переход на страницу авторизации по кнопке "Войти в аккаунт"
        pageAuthorization.inputEmail(InitialData.REGISTRATION_EMAIL); //заполнение email
        pageAuthorization.inputPassword(InitialData.REGISTRATION_PASSWORD); //заполнение пароля
        pageAuthorization.clickOnButtonEnter(); //клик на кнопку Войти
        boolean actualResult = mainPage.buttonPlaceNewOrderIsDisplayed();

        Assert.assertTrue("ошибка авторизации", actualResult);
    }

    @Test
    @DisplayName("авторизация пользователя через кнопку \"Личный кабинет\"")
    public void authorizationWithButtonPersonalRoom() {
        driver.get(InitialData.MAIN_PAGE_URL);
        MainPage mainPage = new MainPage(driver);
        PageAuthorization pageAuthorization = new PageAuthorization(driver);

        mainPage.clickOnButtonPersonalRoom(); //переход на форму авторизации по кнопке "Личный кабинет"
        pageAuthorization.inputEmail(InitialData.REGISTRATION_EMAIL); //заполнение email
        pageAuthorization.inputPassword(InitialData.REGISTRATION_PASSWORD); //заполнение пароля
        pageAuthorization.clickOnButtonEnter(); //клик на кнопку Войти
        boolean actualResult = mainPage.buttonPlaceNewOrderIsDisplayed();

        Assert.assertTrue("ошибка авторизации", actualResult);
    }

    @Test
    @DisplayName("авторизация пользователя через кнопку в форме регистрации")
    public void authorizationWithButtonInFormRegistration() {
        driver.get(InitialData.MAIN_PAGE_URL);
        MainPage mainPage = new MainPage(driver);
        PageAuthorization pageAuthorization = new PageAuthorization(driver);
        PageRegistration pageRegistration = new PageRegistration(driver);

        mainPage.clickOnButtonPersonalRoom(); //переход на форму авторизации по кнопке "Личный кабинет"
        pageAuthorization.clickOnButtonRegister(); //переход на форму регистрации
        pageRegistration.clickOnButtonEnter(); //переход на форму авторзации из формы регистрации
        pageAuthorization.inputEmail(InitialData.REGISTRATION_EMAIL); //заполнение email
        pageAuthorization.inputPassword(InitialData.REGISTRATION_PASSWORD); //заполнение пароля
        pageAuthorization.clickOnButtonEnter(); //клик на кнопку Войти
        boolean actualResult = mainPage.buttonPlaceNewOrderIsDisplayed();

        Assert.assertTrue("ошибка авторизации", actualResult);
    }

    @Test
    @DisplayName("авторизация пользователя через кнопку в форме восстановления пароля")
    public void authorizationWithButtonInFormRecoveryPassword() {
        driver.get(InitialData.MAIN_PAGE_URL);
        MainPage mainPage = new MainPage(driver);
        PageAuthorization pageAuthorization = new PageAuthorization(driver);
        PageRecoveryPassword pageRecoveryPassword = new PageRecoveryPassword(driver);

        mainPage.clickOnButtonPersonalRoom(); //переход на форму авторизации по кнопке "Личный кабинет"
        pageAuthorization.clickOnButtonRecoveryPassword(); //переход на форму восстановления пароля по кнопке "Восстановить пароль"
        pageRecoveryPassword.clickOnButtonEnter(); //переход на форму авторизации из формы восстановления пароля
        pageAuthorization.inputEmail(InitialData.REGISTRATION_EMAIL); //заполнение email
        pageAuthorization.inputPassword(InitialData.REGISTRATION_PASSWORD); //заполнение пароля
        pageAuthorization.clickOnButtonEnter(); //клик на кнопку Войти
        boolean actualResult = mainPage.buttonPlaceNewOrderIsDisplayed();

        Assert.assertTrue("ошибка авторизации", actualResult);
    }

    @After
    public void tearDown() {
        driver.quit(); //закрыть браузер
        clientAPI.deleteUser(InitialData.REGISTRATION_EMAIL, InitialData.REGISTRATION_PASSWORD); //удаление пользователя
    }
}