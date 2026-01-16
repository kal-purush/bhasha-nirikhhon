package com.herokuapp.interactions;

import net.serenitybdd.core.Serenity;
import net.serenitybdd.screenplay.Actor;
import net.serenitybdd.screenplay.Interaction;
import net.serenitybdd.screenplay.Performable;
import net.serenitybdd.screenplay.Tasks;

public class EnterValueToAlertAction implements Interaction {
    private String message;

    public EnterValueToAlertAction(String message) {
        this.message = message;
    }

    public static Performable withTheData(String message) {
        return Tasks.instrumented(EnterValueToAlertAction.class, message);
    }

    @Override
    public <T extends Actor> void performAs(T actor) {
        Serenity.getDriver().switchTo().alert().sendKeys(message);
    }
}