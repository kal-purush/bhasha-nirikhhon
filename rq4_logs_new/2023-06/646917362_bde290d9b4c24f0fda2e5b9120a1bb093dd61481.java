package swaglabs.actions.purchase;

import net.serenitybdd.core.steps.UIInteractionSteps;

public class ConfirmationPage extends UIInteractionSteps {


    public String thankYouMessage() {
        return $(".complete-text").getText();
    }
}