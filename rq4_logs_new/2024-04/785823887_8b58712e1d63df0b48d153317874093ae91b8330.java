package StepDefinitions;

import Pages.DialogContent_Mert;
import Utilities.GWD;
import io.cucumber.java.en.Given;
import org.openqa.selenium.interactions.Actions;

public class US_707_MertSteps {
    DialogContent_Mert dcm=new DialogContent_Mert();
    @Given("Search for the jacket name as Taurus Elements Shell")
    public void searchForTheJacketNameAs() {
        Actions driverAction=new Actions(GWD.getDriver());
        driverAction.moveToElement(dcm.unitMen).build().perform();
        driverAction.moveToElement(dcm.unitTops).build().perform();
        dcm.myClick(dcm.unitJackets);

    }
}