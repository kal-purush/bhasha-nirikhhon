package ru.netology.page;

import com.codeborne.selenide.Condition;
import com.codeborne.selenide.SelenideElement;
import ru.netology.data.DataHelper;

import java.time.Duration;

import static com.codeborne.selenide.Condition.text;
import static com.codeborne.selenide.Condition.visible;
import static com.codeborne.selenide.Selenide.$;

public class FormPage {

    private SelenideElement paymentButton = $("[class^='App_appContainer'] > .button:first-of-type");
    private SelenideElement creditButton = $("[class^='App_appContainer'] > .button:last-of-type");
    private SelenideElement formHead = $("[class^='App_appContainer'] > h3.heading");
    private SelenideElement formBody = $("form");
    private SelenideElement numberField = $("[placeholder='0000 0000 0000 0000']");
    private SelenideElement monthField = $("[placeholder='08']");
    private SelenideElement yearField = $("[placeholder='22']");
    private SelenideElement holderField = $("input:not([placeholder])");
    private SelenideElement cvcField = $("[placeholder='999']");
    private SelenideElement continueButton = $("form button");
    private SelenideElement successNotification = $(".notification_status_ok");
    private SelenideElement errorNotification = $(".notification_status_error");

    private SelenideElement errorNotify(SelenideElement element) {
        return element.closest(".input__inner").find(".input__sub");
    }


    public FormPage() {
        paymentButton.shouldBe(visible);
        creditButton.shouldBe(visible);
        formHead.shouldBe(visible);
        formBody.shouldBe(visible);
        continueButton.shouldBe(visible);
        successNotification.shouldBe(Condition.hidden);
        errorNotification.shouldBe(Condition.hidden);
    }

    public void setValues(DataHelper.CardInfo info) {
        numberField.setValue(info.getNumber());
        monthField.setValue(info.getMonth());
        yearField.setValue(info.getYear());
        holderField.setValue(info.getHolder());
        cvcField.setValue(info.getCvc());
        continueButton.click();
    }

    public void setEmptyValue(DataHelper.CardInfo info, String empty) {
        if (!empty.equals("number")) {
            numberField.setValue(info.getNumber());
        }
        if (!empty.equals("monthField")) {
            monthField.setValue(info.getMonth());
        }
        if (!empty.equals("yearField")) {
            yearField.setValue(info.getYear());
        }
        if (!empty.equals("holderField")) {
            holderField.setValue(info.getHolder());
        }
        if (!empty.equals("cvcField")) {
            cvcField.setValue(info.getCvc());
        }
        continueButton.click();
    }

    public void checkSuccessNotification() {
        successNotification.shouldBe(visible, Duration.ofSeconds(20));
    }

    public void checkErrorNotification() {
        errorNotification.shouldBe(visible, Duration.ofSeconds(20));
    }

    public void checkErrorNotifyIfEmptyNumber() {
        errorNotify(numberField).shouldBe(visible, text("Поле обязательно для заполнения"));
    }

    public void checkErrorNotifyIfInvalidNumber() {
        errorNotify(numberField).shouldBe(visible, text("Неверный формат"));
    }

    public void checkErrorNotifyIfEmptyMonth() {
        errorNotify(monthField).shouldBe(visible, text("Поле обязательно для заполнения"));
    }

    public void checkErrorNotifyIfInvalidMonth() {
        errorNotify(monthField).shouldBe(visible, text("Неверно указан срок действия карты"));
    }

    public void checkErrorNotifyIfExpiredMonth() {
        errorNotify(monthField).shouldBe(visible, text("Истёк срок действия карты"));
    }

    public void checkErrorNotifyIfEmptyYear() {
        errorNotify(yearField).shouldBe(visible, text("Поле обязательно для заполнения"));
    }

    public void checkErrorNotifyIfInvalidYear() {
        errorNotify(yearField).shouldBe(visible, text("Неверно указан срок действия карты"));
    }

    public void checkErrorNotifyIfExpiredYear() {
        errorNotify(yearField).shouldBe(visible, text("Истёк срок действия карты"));
    }

    public void checkErrorNotifyIfEmptyHolder() {
        errorNotify(holderField).shouldBe(visible, text("Поле обязательно для заполнения"));
    }

    public void checkErrorNotifyIfInvalidHolder() {
        errorNotify(holderField).shouldBe(visible, text("Неверный формат"));
    }

    public void checkErrorNotifyIfEmptyCVC() {
        errorNotify(cvcField).shouldBe(visible, text("Поле обязательно для заполнения"));
    }

    public void checkErrorNotifyIfInvalidCVC() {
        errorNotify(cvcField).shouldBe(visible, text("Неверный формат"));
    }
}