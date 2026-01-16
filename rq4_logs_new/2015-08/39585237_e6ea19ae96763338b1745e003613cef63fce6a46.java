package edu.uw.tacoma.mmuppa.webservicelab;

import android.test.ActivityInstrumentationTestCase2;

import com.robotium.solo.Solo;

import java.util.Random;

/**
 * Created by mmuppa on 8/6/15.
 */
public class MainActivityTest extends ActivityInstrumentationTestCase2<MainActivity> {

    private Solo solo;

    public MainActivityTest() {
        super(MainActivity.class);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        solo = new Solo(getInstrumentation(), getActivity());
    }

    @Override
    public void tearDown() throws Exception {
        //tearDown() is run after a test case has finished.
        //finishOpenedActivities() will finish all the activities that have been opened during the test execution.
        solo.finishOpenedActivities();
    }

    public void testDataShowsUp() {
        solo.unlockScreen();
        boolean textFound = solo.searchText("mmuppa");
        assertTrue("User List retrieved", textFound);

    }

    public void testAddUser() {
        solo.clickOnText("Add User");
        boolean textFound = solo.searchText("Enter email address");
        assertTrue("Went to the next fragment", textFound);
    }

    public void testAddUserFragment() {
        solo.clickOnText("Add User");
        Random random = new Random();
        int number = random.nextInt(10000);
        solo.enterText(0, "test@test"+number + ".com");
        solo.enterText(1, "somepassword");
        solo.clickOnButton("Add User");
        boolean textFound = solo.searchText("User successfully inserted");
        assertTrue("User add passed", textFound);
    }

    public void testAddDuplicateUserFragment() {
        solo.clickOnText("Add User");
        solo.enterText(0, "mmuppa@uw.edu");
        solo.enterText(1, "testing");
        solo.clickOnButton("Add User");
        boolean textFound = solo.searchText("Failed :");
        assertTrue("User add failed", textFound);
    }



}