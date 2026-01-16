package lesson_12.pages;

import com.codeborne.selenide.SelenideElement;
import io.qameta.allure.Step;

import static com.codeborne.selenide.Selectors.byText;
import static com.codeborne.selenide.Selenide.*;

public class RegistrationFormPage {

    CalendarComponent calendarComponent = new CalendarComponent();
    ResultModal resultModal = new ResultModal();

    private SelenideElement
            firstNameInput  = $("#firstName"),
            lastNameInput  = $("#lastName"),
            userEmailInput  = $("#userEmail"),
            userNumberInput  = $("#userNumber"),
            subjectsInput = $("#subjectsInput"),
            pictureLoading = $("#uploadPicture"),
            addressInput = $("#currentAddress"),
            submitButton = $("#submit");

    @Step("Открыть страницу /automation-practice-form")
    public RegistrationFormPage openPage(){
        open("https://demoqa.com/automation-practice-form");
        return this;
    }

    @Step("Удалить рекламный баннер")
    public RegistrationFormPage removeBanner(){
        executeJavaScript("$('#fixedban').remove()");
        executeJavaScript("$('footer').remove()");
        return this;
    }

    @Step("Ввести значение {firstName}")
    public RegistrationFormPage setFirstName(String firstName){
        firstNameInput.setValue(firstName);
        return this;
    }

    @Step("")
    public RegistrationFormPage setLastName(String lastName){
        lastNameInput.setValue(lastName);
        return this;
    }

    @Step("")
    public RegistrationFormPage setUserEmail(String userEmail){
        userEmailInput.setValue(userEmail);
        return this;
    }

    @Step("")
    public RegistrationFormPage setGender(String gender){
        $(byText(gender)).click();
        return this;
    }

    @Step("")
    public RegistrationFormPage setUserNumber(String userNumber){
        userNumberInput.setValue(userNumber);
        return this;
    }

    @Step("")
    public RegistrationFormPage setDateOfBirth(String year, String month, String day){
        $(".react-datepicker-wrapper").click();
        calendarComponent.setData(year, month, day);
        return this;
    }

    @Step("")
    public RegistrationFormPage setSubjects(String subjects){
        subjectsInput.setValue(subjects).pressEnter();
        return this;
    }

    @Step("")
    public RegistrationFormPage setHobbies(String hobbies){
        $(byText(hobbies)).parent().click();
        return this;
    }

    @Step("")
    public RegistrationFormPage setPictureLoading(String path){
        pictureLoading.uploadFromClasspath(path);
        return this;
    }

    @Step("")
    public RegistrationFormPage setAddress(String address){
        addressInput.setValue(address);
        return this;
    }

    @Step("")
    public RegistrationFormPage setState(String state){
        $("#state").click();
        $(byText(state)).click();
        return this;
    }

    @Step("")
    public RegistrationFormPage setCity(String city){
        $("#city").click();
        $(byText(city)).click();
        return this;
    }

    @Step("")
    public RegistrationFormPage submit(){
        submitButton.click();
        return this;
    }

    @Step("")
    public RegistrationFormPage verifyModal(){
        resultModal.verifyModal();
        return this;
    }

    @Step("")
    public RegistrationFormPage verifyResult(String key, String value){
        resultModal.verifyResult(key, value);
        return this;
    }
}