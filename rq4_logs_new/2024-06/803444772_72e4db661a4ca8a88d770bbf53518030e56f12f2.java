package StepDefinitions;

import Pages.DialogContent;
import Utilities.Tools;
import io.cucumber.java.en.And;
import io.cucumber.java.en.When;
import org.apache.poi.ss.formula.functions.T;
import org.testng.Assert;

public class _16_GradingSteps {
    DialogContent dc=new DialogContent();
    @When("link to grading")
    public void linkToGrading() {
        Tools.Wait(3);
        dc.myClick(dc.Grading);
    }

    @And("makes sure buttons in the link work properly")
    public void makesSureButtonsInTheLinkWorkProperly() {
        dc.myClick(dc.courseGrade);
        Assert.assertTrue(dc.courseGrade.isDisplayed(),"The icon can not be displayed");
        Tools.Wait(2);
        dc.myClick(dc.StudentTanscript);
        Assert.assertTrue(dc.StudentTanscript.isDisplayed(),"The icon can not be displayed");
        Tools.Wait(2);
        dc.myClick(dc.BySubject);
        Assert.assertTrue(dc.BySubject.isDisplayed(),"The icon can not be displayed");


    }
}