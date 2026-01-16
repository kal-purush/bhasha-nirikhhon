package StepDefinitions;

import Pages.DialogContent;
import Pages.LeftNav;
import io.cucumber.java.en.*;
import org.openqa.selenium.support.ui.Select;

public class _09US_BankAccounts {
    LeftNav ln = new LeftNav();
    DialogContent dc=new DialogContent();

    @Then("Navigate to Bank Accounts")
    public void navigateToBankAccounts() {

        ln.myClick(ln.setup);
        ln.myClick(ln.parameters);
        //ln.myClick(ln.bankAccounts);
    }

    @And("Create a new Bank Accounts")
    public void createANewBankAccounts()  {


        dc.myClick(dc.addButton);
        dc.mySendKeys(dc.nameInput,"deneme");
        dc.myClick(dc.ibanBox);
        dc.mySendKeys(dc.ibanBox,"123123123123");



    }

    @Then("Updata the Bank Accounts")
    public void updataTheBankAccounts() {
    }

    @And("Delete the Bank Accounts")
    public void deleteTheBankAccounts() {
    }
}