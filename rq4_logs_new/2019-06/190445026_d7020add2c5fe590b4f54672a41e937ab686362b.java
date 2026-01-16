package definitions;

import cucumber.api.java.en.Given;
import cucumber.api.java.en.When;
import net.thucydides.core.annotations.Steps;
import steps.PharmacySteps;

public class PharmacyStepDefinitions {

    @Steps
    PharmacySteps pharmacySteps;

    @Given("^user is on pharmacies page$")
    public void userIsOnHomepage() {
        pharmacySteps.verifyPharmaciesPage();
    }

    @When("^user creates a new pharmacy$")
    public void userCreatesANewPharmacy() {

        pharmacySteps.createNewPharmacy();
    }
}