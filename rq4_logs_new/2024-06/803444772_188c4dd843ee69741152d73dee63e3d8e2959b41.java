package StepDefinitions;

import Pages.DialogContent;
import Utilities.Tools;
import io.cucumber.java.en.Then;

public class _13_AttendanceFeature {

    DialogContent dc=new DialogContent();


    @Then("Click on the Attendance Excuses")
    public void clickOnTheAttendanceExcuses() {
        dc.myClick(dc.AttendanceButton);
        Tools.Wait(2);
        dc.myClick(dc.AttendanceExcusesButton);
        Tools.Wait(2);
        dc.myClick(dc.AddIcon);

        dc.myClick(dc.TextBox);
        dc.mySendKeys(dc.TextBox,"I am brokenhearted,I wıll Skıp the class ıf you would excuse me");

        dc.myClick(dc.AttachFile);
        dc.myClick(dc.FromLocal);

       dc.myUploadFile("C:\\Users\\User\\Desktop\\Yeni klasör\\th.jpg");





    }
}

