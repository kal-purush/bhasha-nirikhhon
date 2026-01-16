package lesson_9.pages;

import com.codeborne.selenide.SelenideElement;

import static com.codeborne.selenide.Condition.text;
import static com.codeborne.selenide.Selenide.$;
import static com.codeborne.selenide.Selenide.open;

public class StepOnePage {

    private SelenideElement
            headerForm = $("h2"),
            lastNameInput  = $("#downshift-0-input"),
            firstNameInput  = $("#downshift-2-input"),
            middleNameInput  = $("#downshift-4-input"),
            genderManButton = $("button[value='m']"),
            phoneInput = $("input[name = 'mobilePhone']"),
            emailInput = $("#downshift-6-input"),
            confirmedCheckbox = $(".checkbox__box_1wwku"),
            continueButton = $(".button__m_qvs5j");

    public StepOnePage openPage(){
        open("https://anketa.alfabank.ru/alfaform-refpil/step1");
        return this;
    }

    public StepOnePage verifyHeaderStepOne(String header){
        headerForm.shouldHave(text(header));
        return this;
    }

    public StepOnePage setFirstName(String name){
        firstNameInput.setValue(name);
        return this;
    }

    public StepOnePage setLastName(String name){
        lastNameInput.setValue(name);
        return this;
    }

    public StepOnePage setMiddleName(String name){
        middleNameInput.setValue(name);
        $("#downshift-5-item-0 >div").click();
        return this;
    }

    public StepOnePage setPhone(String phone){
        phoneInput.setValue(phone);
        return this;
    }

    public StepOnePage setEmail(String email){
        emailInput.setValue(email).pressEnter();
        return this;
    }

    public StepOnePage clickContinueButton(){
        continueButton.click();
        return this;
    }
    public StepOnePage setConfirmedCheckbox(){
        confirmedCheckbox.click();
        return this;
    }
}