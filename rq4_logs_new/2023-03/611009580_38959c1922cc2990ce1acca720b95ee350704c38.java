package com.herokuapp.questions;

import net.serenitybdd.screenplay.Actor;
import net.serenitybdd.screenplay.Question;

import static com.herokuapp.userinterfaces.MultipleWindowsPage.LABEL_MESSAGE_NEW_WINDOW;

public class ValidatePhraseWindows implements Question<Boolean> {
    private String phrase;

    public ValidatePhraseWindows(String phrase) {
        this.phrase = phrase;
    }

    public static ValidatePhraseWindows verify(String phrase) {
        return new ValidatePhraseWindows(phrase);
    }

    @Override
    public Boolean answeredBy(Actor actor) {
        return LABEL_MESSAGE_NEW_WINDOW.resolveFor(actor).getTextContent().contains(phrase);
    }
}