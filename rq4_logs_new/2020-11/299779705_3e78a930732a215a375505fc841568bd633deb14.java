package com.example.cmput301f20t18;

import androidx.test.platform.app.InstrumentationRegistry;
import androidx.test.rule.ActivityTestRule;

import com.robotium.solo.Solo;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class ChooseLocationActivityTest {
    private Solo solo;

    @Rule
    public ActivityTestRule<Login> rule =
            new ActivityTestRule<>(Login.class, true, true);
    @Before
    public void setUp() throws Exception{
        solo = new Solo(InstrumentationRegistry.getInstrumentation(), rule.getActivity());
        RobotiumLoginManager.loginOwner(solo);
        solo.clickOnView(solo.getView(R.id.tab_mybooks));
        RobotiumUserBookManager.add(solo);
        RobotiumLoginManager.signOut(solo);

        RobotiumLoginManager.loginBorrower(solo);
        RobotiumTransactionManager.requestBook(solo, RobotiumUserBookManager.DEFAULT_BOOK_TITLE,
                RobotiumUserBookManager.DEFAULT_BOOK_TITLE);
        RobotiumLoginManager.signOut(solo);
        RobotiumLoginManager.loginOwner(solo);

        solo.clickOnView(solo.getView(R.id.tab_mybooks));
        solo.clickOnButton("View requests");
        solo.clickOnButton("Accept");
    }
    @After
    public void cleanUp(){
        solo.goBack();
        RobotiumUserBookManager.deleteAll(solo);
    }
    @Test
    public void start(){
        solo.assertCurrentActivity("Wrong Activity - NOT CHOOSELOCATION",
                ChooseLocationActivity.class);
    }
    @Test
    public void cancelButton(){
        solo.assertCurrentActivity("Wrong Activity - NOT CHOOSELOCATION",
                ChooseLocationActivity.class);
        solo.clickOnView(solo.getView(R.id.button_back));
        solo.assertCurrentActivity("Wrong Activity - NOT HOMESCREEN",
                HomeScreen.class);
    }
    @Test
    public void AddNewLocationButton(){
        solo.assertCurrentActivity("Wrong Activity - NOT CHOOSELOCATIONACTIVITY",
                ChooseLocationActivity.class);
        solo.clickOnButton("Add New Location");
        solo.assertCurrentActivity("Wrong Activity - NOT SELECTLOCATIONACTIVITY",
                SelectLocationActivity.class);
        solo.goBack();
    }
    @Test
    public void AddLocation(){
        solo.assertCurrentActivity("Wrong Activity - NOT CHOOSELOCATIONACTIVITY",
                ChooseLocationActivity.class);
        solo.clickOnButton("Add New Location");
        solo.assertCurrentActivity("Wrong Activity - NOT SELECTLOCATIONACTIVITY",
                SelectLocationActivity.class);
        solo.clickOnView(solo.getView(R.id.confirm_location_selected_button));

        assertTrue(solo.waitForText("Select", 1, 2000));
        RobotiumLocationManager.deleteAll(solo);
    }
    //Requires delete books to delete all in pending
//    @Test
//    public void selectLocation(){
//
//    }
}