package StepDefinitions;

import Pages.DialogContent;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;

public class _10_HamburgerMenuFinance {

    DialogContent dc=new DialogContent();
    @Then("Click on the Finance and My Finance and choose online method and stripe")
    public void clickOnTheFinanceAndMyFinanceAndChooseOnlineMethodAndStripe()  {
        dc.myClick(dc.hamburger);
        dc.myClick(dc.financeLink);
        dc.myClick(dc.myfinanceLink);
        dc.myClick(dc.ChooseStudent);

        dc.verifyContainsText(dc.AssertOnlinePay,"Online Payment");
        dc.myClick(dc.StripeButton);


    }


    @When("the student selects Stripe and makes a payment")
    public void theStudentSelectsStripeAndMakesAPayment() {
        dc.myClick(dc.Pay);
        dc.mySendKeys(dc.CardNumber,"4242 4242 4242 4242 ");
        dc.mySendKeys(dc.Expiration,"1128");
        dc.mySendKeys(dc.Cvc,"000");
        dc.myClick(dc.StripeElement);
        dc.myClick(dc.balanceButton);
        dc.verifyContainsText(dc.AsserPayerInfo,"Payer Info");




    }
}