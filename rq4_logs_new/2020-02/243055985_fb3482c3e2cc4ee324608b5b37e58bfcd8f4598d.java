package step_definitions.briteERPEmployeeModule;

import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import org.junit.Assert;
import pages.EmployeeFollowPage;
import utilities.Driver;
import utilities.Reusable;

public class Follow_unfollow_anyemploye {

    EmployeeFollowPage followPage = new EmployeeFollowPage();




    @Given("User is able to see employees")
    public void user_is_able_to_see_employees() {
    String expectedTitle = "Employees - Odoo";
    String actualTitle = Driver.getDriver().getTitle();
        Assert.assertEquals("Employees are not visible",expectedTitle,actualTitle);
    }

    @Given("User is able to locate following or follow button")
    public void user_is_able_to_locate_following_or_follow_button() {
        for (int i = 0; i < followPage.followButton.size(); i++) {
            Assert.assertTrue("Button is not displayed", followPage.followButton.get(i).isDisplayed());
        }
    }

    @Given("User clicks on the follow or unfollow button")
    public void user_clicks_on_the_follow_or_unfollow_button() {
            if (followPage.following > 0) followPage.unfollowAll();
            Assert.assertEquals(followPage.following, 0);
            Reusable.pause(3);
            followPage.followAll();
            Assert.assertEquals("Test is not passed, user is unable to follow other employees!",35,followPage.following );

        }

    @Then("User can see that the text on follow button changed to following")
    public void user_can_see_that_the_text_on_follow_button_changed_to_following() {
        for (int i = 0; i < followPage.followButton.size(); i++) {


            Assert.assertEquals("Sorry the button still follow", "following", followPage.followButton.get(i).getText());
        }
    }

}