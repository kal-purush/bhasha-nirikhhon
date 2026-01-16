package com.herokuapp.questions;

import net.serenitybdd.screenplay.Actor;
import net.serenitybdd.screenplay.Question;

import static com.herokuapp.userinterfaces.JavaScriptAlertsPage.LABEL_MESSAGE;

public class ValidatePhraseAlerts implements Question<Boolean> {
    private String phrase;

    public ValidatePhraseAlerts(String phrase) {
        this.phrase = phrase;
    }

    public static ValidatePhraseAlerts verify(String phrase){
        return new ValidatePhraseAlerts(phrase);

    }
    @Override
    public Boolean answeredBy(Actor actor) {
        return LABEL_MESSAGE.resolveFor(actor).getTextContent().contains(phrase);
    }
}